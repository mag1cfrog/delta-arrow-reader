use super::*;

#[test]
fn native_file_aliases_resolve_without_rewriting_credentials_or_endpoints()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = std::path::absolute("credentials")?;
    let absolute = directory
        .join("absolute.jwt")
        .into_os_string()
        .into_string()
        .map_err(|_| "non-UTF-8 test path")?;
    for (scheme, keys) in [
        (
            ObjectStoreScheme::AmazonS3,
            vec![
                "WEB_IDENTITY_TOKEN_FILE",
                "aws_container_authorization_token_file",
            ],
        ),
        (
            ObjectStoreScheme::MicrosoftAzure,
            vec!["AZURE_FEDERATED_TOKEN_FILE"],
        ),
        (
            ObjectStoreScheme::GoogleCloudStorage,
            vec!["APPLICATION_CREDENTIALS"],
        ),
    ] {
        for key in keys {
            for path in ["../identity/secret.jwt", absolute.as_str()] {
                let mut options = DeltaStorageOptions::from([
                    (key.to_owned(), path.to_owned()),
                    (
                        "proxy_url".to_owned(),
                        "https://proxy.example.test".to_owned(),
                    ),
                    (
                        "secret_access_key".to_owned(),
                        "../literal-secret".to_owned(),
                    ),
                ]);
                resolve_relative_paths(&mut options, &scheme, &directory)?;
                assert_eq!(Path::new(&options[key]), directory.join(path));
                assert_eq!(options["proxy_url"], "https://proxy.example.test");
                assert_eq!(options["secret_access_key"], "../literal-secret");
            }
        }
    }
    Ok(())
}

#[test]
fn invalid_references_cannot_become_valid_paths_after_joining() {
    for value in ["", " ", "secret\npath", "secret\0path"] {
        let mut options =
            DeltaStorageOptions::from([("web_identity_token_file".into(), value.into())]);
        let result = resolve_relative_paths(
            &mut options,
            &ObjectStoreScheme::AmazonS3,
            Path::new("credentials"),
        );
        assert!(matches!(
            result,
            Err(Error::StorageAuthorization {
                reason: "auth_source_invalid_value"
            })
        ));
    }
}
