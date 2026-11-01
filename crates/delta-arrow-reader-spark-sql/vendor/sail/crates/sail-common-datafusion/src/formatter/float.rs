//! Spark uses the same FLOAT/DOUBLE text in casts and structured output.
//! For example, 1e10 becomes "1.0E10", and negative zero stays "-0.0".

macro_rules! define_float_formatter {
    ($name:ident, $type:ty) => {
        /// Render a floating-point value with Spark's Java 21 notation and rounding.
        pub fn $name(value: $type) -> String {
            if value.is_nan() {
                return "NaN".into();
            }
            let sign = if value.is_sign_negative() { "-" } else { "" };
            if value.is_infinite() {
                return format!("{sign}Infinity");
            }
            if value == 0.0 {
                return format!("{sign}0.0");
            }
            let mut buffer = ryu::Buffer::new();
            let (mut digits, mut exponent) =
                decimal_significand_and_exponent(buffer.format_finite(value.abs()));
            if digits.len() == 1 {
                // Java chooses the closest one- or two-digit decimal in this case.
                (digits, exponent) =
                    decimal_significand_and_exponent(&format!("{:.1e}", value.abs()));
            }
            format_float_notation(sign, &digits, exponent)
        }
    };
}

define_float_formatter!(format_float32, f32);
define_float_formatter!(format_float64, f64);

fn decimal_significand_and_exponent(text: &str) -> (String, i32) {
    // Input comes from finite positive ryu/Rust formatting, never SQL text.
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text, "0"));
    let exponent = exponent.parse::<i32>().unwrap_or_default();
    let integer_digits = mantissa.find('.').unwrap_or(mantissa.len());
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let leading_zeroes = digits.len() - digits.trim_start_matches('0').len();
    (
        digits.trim_matches('0').into(),
        exponent + integer_digits as i32 - leading_zeroes as i32 - 1,
    )
}

fn format_float_notation(sign: &str, digits: &str, exponent: i32) -> String {
    if (-3..7).contains(&exponent) {
        if exponent < 0 {
            format!("{sign}0.{}{}", "0".repeat((-exponent - 1) as usize), digits)
        } else {
            let integral = exponent as usize + 1;
            if integral >= digits.len() {
                format!("{sign}{}{}.0", digits, "0".repeat(integral - digits.len()))
            } else {
                format!("{sign}{}.{}", &digits[..integral], &digits[integral..])
            }
        }
    } else if digits.len() == 1 {
        format!("{sign}{digits}.0E{exponent}")
    } else {
        format!("{sign}{}.{}E{exponent}", &digits[..1], &digits[1..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float64_uses_java_notation() {
        // Values pinned against Java `Double.toString` (Spark JVM ground truth).
        assert_eq!(format_float64(f64::NAN), "NaN");
        assert_eq!(format_float64(f64::INFINITY), "Infinity");
        assert_eq!(format_float64(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(format_float64(0.0), "0.0");
        assert_eq!(format_float64(-0.0), "-0.0");
        assert_eq!(format_float64(1.0), "1.0");
        assert_eq!(format_float64(100.0), "100.0");
        assert_eq!(format_float64(2.71), "2.71");
        assert_eq!(format_float64(-123.456), "-123.456");
        assert_eq!(format_float64(0.001), "0.001");
        assert_eq!(format_float64(0.0009), "9.0E-4");
        assert_eq!(format_float64(0.0001), "1.0E-4");
        assert_eq!(format_float64(9999999.0), "9999999.0");
        assert_eq!(format_float64(10000000.0), "1.0E7");
        assert_eq!(format_float64(12345678.0), "1.2345678E7");
        assert_eq!(format_float64(1e20), "1.0E20");
        assert_eq!(format_float64(1e-7), "1.0E-7");
        assert_eq!(format_float64(1e308), "1.0E308");
        assert_eq!(format_float64(123456789012345.0), "1.23456789012345E14");
    }

    #[test]
    fn float32_uses_java_notation() {
        assert_eq!(format_float32(f32::NAN), "NaN");
        assert_eq!(format_float32(f32::INFINITY), "Infinity");
        assert_eq!(format_float32(1.0), "1.0");
        assert_eq!(format_float32(0.1), "0.1");
        assert_eq!(format_float32(123.456), "123.456");
        assert_eq!(format_float32(10000000.0), "1.0E7");
    }
}
