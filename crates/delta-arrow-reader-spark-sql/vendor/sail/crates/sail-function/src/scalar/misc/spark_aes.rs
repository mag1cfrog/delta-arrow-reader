// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.

use std::borrow::Cow;
use std::sync::Arc;

use aes::cipher::block_padding::{Pkcs7, RawPadding};
use aes::cipher::consts::{U12, U16};
use aes::cipher::{
    Block, BlockCipher, BlockDecrypt, BlockDecryptMut, BlockEncrypt, BlockEncryptMut, KeyIvInit,
};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::rand_core::{OsRng, RngCore};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, Nonce};
use datafusion::arrow::array::{ArrayRef, BinaryBuilder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

type Aes192Gcm = AesGcm<Aes192, U12>;
const AES_BLOCK_BYTES: usize = 16;

#[derive(Clone, Copy)]
enum AesMode {
    Ecb,
    Cbc,
    Gcm,
}

impl AesMode {
    fn parse(value: &str) -> Result<Self> {
        let name = value.trim();
        if name.eq_ignore_ascii_case("ECB") {
            Ok(Self::Ecb)
        } else if name.eq_ignore_ascii_case("CBC") {
            Ok(Self::Cbc)
        } else if name.is_empty() || name.eq_ignore_ascii_case("GCM") {
            Ok(Self::Gcm)
        } else {
            exec_err!("Spark AES mode must be ECB, CBC or GCM")
        }
    }

    fn iv_len(self) -> usize {
        match self {
            Self::Ecb => 0,
            Self::Cbc => AES_BLOCK_BYTES,
            Self::Gcm => 12,
        }
    }

    fn validate_padding(self, padding: &str) -> Result<()> {
        let mode_padding = if matches!(self, Self::Gcm) {
            "NONE"
        } else {
            "PKCS"
        };
        if padding.eq_ignore_ascii_case("DEFAULT") || padding.eq_ignore_ascii_case(mode_padding) {
            Ok(())
        } else {
            exec_err!("Spark AES padding is unsupported for the selected mode")
        }
    }
}

#[derive(Clone, Copy)]
enum AesOperation {
    Encrypt,
    Decrypt,
}

impl AesOperation {
    fn sql_name(self) -> &'static str {
        match self {
            Self::Encrypt => "aes_encrypt",
            Self::Decrypt => "aes_decrypt",
        }
    }

    fn max_args(self) -> usize {
        match self {
            Self::Encrypt => 6,
            Self::Decrypt => 5,
        }
    }
}

#[derive(Clone, Copy)]
enum AesErrorPolicy {
    ReturnError,
    ReturnNull,
}

fn binary_argument<'a>(value: &'a ScalarValue, name: &str) -> Result<&'a [u8]> {
    match value {
        ScalarValue::Binary(Some(bytes))
        | ScalarValue::BinaryView(Some(bytes))
        | ScalarValue::LargeBinary(Some(bytes))
        | ScalarValue::FixedSizeBinary(_, Some(bytes)) => Ok(bytes),
        ScalarValue::Utf8(Some(text))
        | ScalarValue::Utf8View(Some(text))
        | ScalarValue::LargeUtf8(Some(text)) => Ok(text.as_bytes()),
        _ => exec_err!("Spark AES {name} must be BINARY or STRING"),
    }
}

fn string_argument<'a>(value: &'a ScalarValue, name: &str) -> Result<&'a str> {
    match value {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::Utf8View(Some(text))
        | ScalarValue::LargeUtf8(Some(text)) => Ok(text),
        _ => exec_err!("Spark AES {name} must be STRING"),
    }
}

fn apply_gcm<C: Aead<NonceSize = U12> + KeyInit>(
    operation: AesOperation,
    key: &[u8],
    iv: &[u8],
    message: &[u8],
    associated_data: &[u8],
) -> Result<Vec<u8>> {
    let cipher = C::new_from_slice(key)
        .map_err(|_| exec_datafusion_err!("Spark AES GCM key length is invalid"))?;
    let nonce = Nonce::from_slice(iv);
    let payload = Payload {
        msg: message,
        aad: associated_data,
    };
    let result = match operation {
        AesOperation::Encrypt => cipher.encrypt(nonce, payload),
        AesOperation::Decrypt => cipher.decrypt(nonce, payload),
    };
    result.map_err(|_| exec_datafusion_err!("Spark AES GCM {} failed", operation.sql_name()))
}

fn apply_ecb<C>(operation: AesOperation, key: &[u8], input: &[u8]) -> Result<Vec<u8>>
where
    C: BlockCipher<BlockSize = U16> + BlockEncrypt + BlockDecrypt + KeyInit,
{
    let cipher = C::new_from_slice(key)
        .map_err(|_| exec_datafusion_err!("Spark AES ECB key length is invalid"))?;
    apply_ecb_blocks(operation, &cipher, input)
}

fn apply_ecb_blocks<C>(operation: AesOperation, cipher: &C, input: &[u8]) -> Result<Vec<u8>>
where
    C: BlockCipher<BlockSize = U16> + BlockEncrypt + BlockDecrypt,
{
    let mut bytes = input.to_vec();
    match operation {
        AesOperation::Encrypt => {
            let remainder = input.len() % AES_BLOCK_BYTES;
            let last_block = input.len() - remainder;
            bytes.resize(last_block + AES_BLOCK_BYTES, 0);
            Pkcs7::raw_pad(&mut bytes[last_block..], remainder);
            for block in bytes.chunks_exact_mut(AES_BLOCK_BYTES) {
                cipher.encrypt_block(Block::<C>::from_mut_slice(block));
            }
        }
        AesOperation::Decrypt => {
            if bytes.is_empty() {
                return Ok(bytes);
            }
            if !bytes.len().is_multiple_of(AES_BLOCK_BYTES) {
                return exec_err!("Spark AES ECB ciphertext has an invalid length");
            }
            for block in bytes.chunks_exact_mut(AES_BLOCK_BYTES) {
                cipher.decrypt_block(Block::<C>::from_mut_slice(block));
            }
            let last_block = bytes.len() - AES_BLOCK_BYTES;
            let tail = Pkcs7::raw_unpad(&bytes[last_block..]).map_err(|_| {
                exec_datafusion_err!("Spark AES ECB ciphertext has invalid padding")
            })?;
            bytes.truncate(last_block + tail.len());
        }
    }
    Ok(bytes)
}

fn apply_block_mode<C>(
    operation: AesOperation,
    mode: AesMode,
    key: &[u8],
    iv: &[u8],
    message: &[u8],
) -> Result<Vec<u8>>
where
    C: BlockCipher<BlockSize = U16> + BlockEncrypt + BlockDecrypt + KeyInit,
{
    // Spark's block modes accept an empty ciphertext body. GCM still requires its tag.
    if matches!(operation, AesOperation::Decrypt) && message.is_empty() {
        return Ok(Vec::new());
    }
    if matches!(mode, AesMode::Ecb) {
        return apply_ecb::<C>(operation, key, message);
    }
    let invalid_key_or_iv = |_| exec_datafusion_err!("Spark AES CBC key or IV length is invalid");
    match operation {
        AesOperation::Encrypt => Ok(cbc::Encryptor::<C>::new_from_slices(key, iv)
            .map_err(invalid_key_or_iv)?
            .encrypt_padded_vec_mut::<Pkcs7>(message)),
        AesOperation::Decrypt => cbc::Decryptor::<C>::new_from_slices(key, iv)
            .map_err(invalid_key_or_iv)?
            .decrypt_padded_vec_mut::<Pkcs7>(message)
            .map_err(|_| exec_datafusion_err!("Spark AES CBC ciphertext is invalid")),
    }
}

fn apply_aes_mode(
    operation: AesOperation,
    mode: AesMode,
    key: &[u8],
    iv: &[u8],
    message: &[u8],
    associated_data: &[u8],
) -> Result<Vec<u8>> {
    match (mode, key.len()) {
        (AesMode::Gcm, 16) => apply_gcm::<Aes128Gcm>(operation, key, iv, message, associated_data),
        (AesMode::Gcm, 24) => apply_gcm::<Aes192Gcm>(operation, key, iv, message, associated_data),
        (AesMode::Gcm, 32) => apply_gcm::<Aes256Gcm>(operation, key, iv, message, associated_data),
        (_, 16) => apply_block_mode::<Aes128>(operation, mode, key, iv, message),
        (_, 24) => apply_block_mode::<Aes192>(operation, mode, key, iv, message),
        (_, 32) => apply_block_mode::<Aes256>(operation, mode, key, iv, message),
        _ => exec_err!("Spark AES key length must be 16, 24 or 32 bytes"),
    }
}

fn evaluate_aes_row(
    operation: AesOperation,
    arguments: &[Cow<'_, ScalarValue>],
) -> Result<Option<Vec<u8>>> {
    if arguments.iter().any(|value| value.is_null()) {
        return Ok(None);
    }
    let message = binary_argument(&arguments[0], "input")?;
    let key = binary_argument(&arguments[1], "key")?;
    let mode = match arguments.get(2) {
        Some(value) => AesMode::parse(string_argument(value, "mode")?)?,
        None => AesMode::Gcm,
    };
    if let Some(padding) = arguments.get(3) {
        mode.validate_padding(string_argument(padding, "padding")?)?;
    }
    let aad_position = match operation {
        AesOperation::Encrypt => 5,
        AesOperation::Decrypt => 4,
    };
    let associated_data = arguments
        .get(aad_position)
        .map(|value| binary_argument(value, "associated data"))
        .transpose()?
        .unwrap_or_default();
    // Spark accepts the ECB decrypt argument and never passes it to the block cipher.
    let accepts_associated_data = matches!(
        (operation, mode),
        (_, AesMode::Gcm) | (AesOperation::Decrypt, AesMode::Ecb)
    );
    if !associated_data.is_empty() && !accepts_associated_data {
        return exec_err!("Spark AES associated data requires GCM mode");
    }
    let iv_bytes = mode.iv_len();
    let result = match operation {
        AesOperation::Encrypt => {
            let supplied_iv = arguments
                .get(4)
                .map(|value| binary_argument(value, "IV"))
                .transpose()?
                .unwrap_or_default();
            let mut generated_iv = [0; AES_BLOCK_BYTES];
            let iv = if supplied_iv.is_empty() {
                let iv = &mut generated_iv[..iv_bytes];
                if !iv.is_empty() {
                    OsRng
                        .try_fill_bytes(iv)
                        .map_err(|_| exec_datafusion_err!("Spark AES could not generate an IV"))?;
                }
                iv
            } else if iv_bytes != 0 && supplied_iv.len() == iv_bytes {
                supplied_iv
            } else {
                return exec_err!("Spark AES IV length is invalid for the selected mode");
            };
            let mut ciphertext = iv.to_vec();
            ciphertext.extend(apply_aes_mode(
                operation,
                mode,
                key,
                iv,
                message,
                associated_data,
            )?);
            ciphertext
        }
        AesOperation::Decrypt => {
            let (iv, ciphertext) = message.split_at_checked(iv_bytes).ok_or_else(|| {
                exec_datafusion_err!("Spark AES ciphertext is shorter than its IV")
            })?;
            apply_aes_mode(operation, mode, key, iv, ciphertext, associated_data)?
        }
    };
    Ok(Some(result))
}

fn evaluate_literal_ecb(
    args: &[ColumnarValue],
    operation: AesOperation,
) -> Option<Result<ColumnarValue>> {
    let [
        ColumnarValue::Array(input),
        ColumnarValue::Scalar(key),
        ColumnarValue::Scalar(mode),
    ] = args
    else {
        return None;
    };
    if input.data_type() != &DataType::Binary
        || !matches!(
            AesMode::parse(string_argument(mode, "mode").ok()?).ok()?,
            AesMode::Ecb
        )
    {
        return None;
    }
    let key = binary_argument(key, "key").ok()?;
    let result = match key.len() {
        16 => evaluate_ecb_column(input, &Aes128::new_from_slice(key).ok()?, operation),
        24 => evaluate_ecb_column(input, &Aes192::new_from_slice(key).ok()?, operation),
        32 => evaluate_ecb_column(input, &Aes256::new_from_slice(key).ok()?, operation),
        _ => return None,
    };
    Some(result)
}

fn evaluate_ecb_column<C>(
    input: &ArrayRef,
    cipher: &C,
    operation: AesOperation,
) -> Result<ColumnarValue>
where
    C: BlockCipher<BlockSize = U16> + BlockEncrypt + BlockDecrypt,
{
    let mut builder = BinaryBuilder::new();
    for row in 0..input.len() {
        let value = ScalarValue::try_from_array(input, row)?;
        let result = if value.is_null() {
            None
        } else {
            binary_argument(&value, "input")
                .and_then(|message| apply_ecb_blocks(operation, cipher, message))
                .ok()
        };
        builder.append_option(result);
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn evaluate_aes(
    args: ScalarFunctionArgs,
    operation: AesOperation,
    on_error: AesErrorPolicy,
) -> Result<ColumnarValue> {
    if !(2..=operation.max_args()).contains(&args.args.len()) {
        if matches!(on_error, AesErrorPolicy::ReturnNull) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
        }
        return exec_err!(
            "Spark `{}` requires 2 to {} arguments, got {}",
            operation.sql_name(),
            operation.max_args(),
            args.args.len()
        );
    }
    if args.args.iter().any(
        |value| matches!(value, ColumnarValue::Array(array) if array.len() != args.number_rows),
    ) {
        return internal_err!("AES argument length differs from the batch row count");
    }
    if matches!(on_error, AesErrorPolicy::ReturnNull)
        && let Some(result) = evaluate_literal_ecb(&args.args, operation)
    {
        return result;
    }
    let evaluate_row = |index| {
        let values = args
            .args
            .iter()
            .map(|value| match value {
                ColumnarValue::Scalar(scalar) => Ok(Cow::Borrowed(scalar)),
                ColumnarValue::Array(array) => {
                    ScalarValue::try_from_array(array, index).map(Cow::Owned)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        match evaluate_aes_row(operation, &values) {
            Err(_) if matches!(on_error, AesErrorPolicy::ReturnNull) => Ok(None),
            result => result,
        }
    };
    if args
        .args
        .iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)))
    {
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(evaluate_row(0)?)));
    }
    let mut builder = BinaryBuilder::new();
    for index in 0..args.number_rows {
        builder.append_option(evaluate_row(index)?);
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

/// Encrypt data using Spark's AES modes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAESEncrypt {
    signature: Signature,
}

impl Default for SparkAESEncrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAESEncrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SparkAESEncrypt {
    fn name(&self) -> &str {
        "spark_aes_encrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `AesEncrypt` is `RuntimeReplaceable` over a `StaticInvoke` that leaves
    /// `returnNullable` at its `true` default, so `nullable = true` whatever the arguments.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/objects/objects.scala#L334>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 6 {
            return exec_err!(
                "Spark `aes_encrypt` function requires 2 to 6 arguments, got {}",
                args.arg_fields.len()
            );
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        evaluate_aes(args, AesOperation::Encrypt, AesErrorPolicy::ReturnError)
    }
}

/// Decrypt data using Spark's AES modes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAESDecrypt {
    signature: Signature,
}

impl Default for SparkAESDecrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAESDecrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkAESDecrypt {
    fn name(&self) -> &str {
        "spark_aes_decrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `AesDecrypt` is `RuntimeReplaceable` over a `StaticInvoke` that leaves
    /// `returnNullable` at its `true` default, so `nullable = true` whatever the arguments.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/objects/objects.scala#L334>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 5 {
            return exec_err!(
                "Spark `aes_decrypt` function requires 2 to 5 arguments, got {}",
                args.arg_fields.len()
            );
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        evaluate_aes(args, AesOperation::Decrypt, AesErrorPolicy::ReturnError)
    }
}

/// Encrypt data, returning NULL when an individual row fails.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryAESEncrypt {
    signature: Signature,
}

impl Default for SparkTryAESEncrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryAESEncrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SparkTryAESEncrypt {
    fn name(&self) -> &str {
        "spark_try_aes_encrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TryEval.nullable = true`, unconditional; `try_aes_encrypt` has no expression of
    /// its own and follows its `TryAesDecrypt` sibling, which wraps the strict call in `TryEval`.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        evaluate_aes(args, AesOperation::Encrypt, AesErrorPolicy::ReturnNull)
    }
}

/// Decrypt data, returning NULL when an individual row fails.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryAESDecrypt {
    signature: Signature,
}

impl Default for SparkTryAESDecrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryAESDecrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryAESDecrypt {
    fn name(&self) -> &str {
        "spark_try_aes_decrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TryAesDecrypt` is `RuntimeReplaceable` over `TryEval(AesDecrypt(...))`, whose
    /// `nullable = true` is unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        evaluate_aes(args, AesOperation::Decrypt, AesErrorPolicy::ReturnNull)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, BinaryArray};

    use super::*;

    #[test]
    fn literal_ecb_keys_match_the_existing_row_evaluation() -> Result<()> {
        let plaintext = b"a partially filled block";
        for key_len in [16, 24, 32] {
            let key = vec![b'x'; key_len];
            let ciphertext = apply_aes_mode(
                AesOperation::Encrypt,
                AesMode::Ecb,
                &key,
                &[],
                plaintext,
                &[],
            )?;
            let data = BinaryArray::from(vec![
                None,
                Some(&[][..]),
                Some(ciphertext.as_slice()),
                Some(&[0][..]),
            ]);
            for operation in [AesOperation::Encrypt, AesOperation::Decrypt] {
                for length in [0, 1, data.len()] {
                    let invoke = |mode: ColumnarValue| {
                        let args = vec![
                            ColumnarValue::Array(Arc::new(data.slice(0, length))),
                            ColumnarValue::Scalar(ScalarValue::Binary(Some(key.clone()))),
                            mode,
                        ];
                        evaluate_aes(
                            ScalarFunctionArgs {
                                arg_fields: args
                                    .iter()
                                    .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                                    .collect(),
                                args,
                                number_rows: length,
                                return_field: Arc::new(Field::new(
                                    "result",
                                    DataType::Binary,
                                    true,
                                )),
                                config_options: Arc::new(Default::default()),
                            },
                            operation,
                            AesErrorPolicy::ReturnNull,
                        )
                    };
                    let mode = ScalarValue::Utf8(Some(" eCb ".to_string()));
                    let expected = invoke(ColumnarValue::Array(mode.to_array_of_size(length)?))?;
                    let actual = invoke(ColumnarValue::Scalar(mode))?;
                    let (ColumnarValue::Array(actual), ColumnarValue::Array(expected)) =
                        (actual, expected)
                    else {
                        panic!("processing a binary column must return an array");
                    };
                    assert_eq!(actual.to_data(), expected.to_data());
                    if matches!(operation, AesOperation::Decrypt) && length == data.len() {
                        let decrypted = actual.as_binary::<i32>();
                        assert!(decrypted.is_null(0));
                        assert_eq!(decrypted.value(1), b"");
                        assert_eq!(decrypted.value(2), plaintext);
                        assert!(decrypted.is_null(3));
                    }
                }
            }
        }
        Ok(())
    }
}
