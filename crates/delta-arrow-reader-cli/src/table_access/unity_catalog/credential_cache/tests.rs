use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn aws_grant(token: &str, lifetime: Duration) -> TemporaryTableCredentials {
    TemporaryTableCredentials {
        credentials: CloudCredentials::Aws(Arc::new(AwsCredential {
            key_id: "secret-key".into(),
            secret_key: "secret-signing-key".into(),
            token: Some(token.into()),
        })),
        expires_at: SystemTime::now() + lifetime,
    }
}

fn cache(grant: TemporaryTableCredentials) -> Arc<TableCredentialCache> {
    TableCredentialCache::new(
        "secret-profile".into(),
        "secret-table-id".into(),
        TableStorageLocation::parse("s3://secret-bucket/secret-table").unwrap(),
        grant,
    )
    .unwrap()
}

fn aws_token(credentials: CloudCredentials) -> String {
    let CloudCredentials::Aws(credentials) = credentials else {
        panic!("expected AWS credentials");
    };
    credentials.token.clone().unwrap()
}

#[tokio::test(start_paused = true)]
async fn concurrent_reads_share_one_refresh_and_reuse_the_replacement() {
    let cache = cache(aws_grant("initial", Duration::from_secs(300)));
    tokio::time::advance(Duration::from_secs(269)).await;
    assert_eq!(aws_token(cache.get_credentials().await.unwrap()), "initial");
    tokio::time::advance(Duration::from_secs(2)).await;

    let refreshes = Arc::new(AtomicUsize::new(0));
    let mut readers = JoinSet::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        let refreshes = Arc::clone(&refreshes);
        readers.spawn(async move {
            cache
                .get_or_refresh(|| async {
                    refreshes.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    Ok(aws_grant("renewed", Duration::from_secs(3600)))
                })
                .await
        });
    }
    while let Some(result) = readers.join_next().await {
        assert_eq!(aws_token(result.unwrap().unwrap()), "renewed");
    }
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    let DeltaStorageCredentialProvider::Aws(provider) = cache.credential_provider() else {
        panic!("expected AWS provider");
    };
    assert_eq!(
        provider.get_credential().await.unwrap().token.as_deref(),
        Some("renewed")
    );
    assert!(cache.refresh_failure().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn short_grants_use_a_proportional_margin_and_long_grants_cap_it() {
    for (lifetime, fresh_for, refresh_after) in [(10, 8, 2), (3600, 3569, 2)] {
        let cache = cache(aws_grant("initial", Duration::from_secs(lifetime)));
        tokio::time::advance(Duration::from_secs(fresh_for)).await;
        assert_eq!(aws_token(cache.get_credentials().await.unwrap()), "initial");
        tokio::time::advance(Duration::from_secs(refresh_after)).await;
        assert_eq!(
            aws_token(
                cache
                    .get_or_refresh(|| async {
                        Ok(aws_grant("renewed", Duration::from_secs(lifetime)))
                    })
                    .await
                    .unwrap()
            ),
            "renewed"
        );
        // A short replacement must not immediately trigger another subprocess.
        assert_eq!(aws_token(cache.get_credentials().await.unwrap()), "renewed");
    }
}

#[tokio::test(start_paused = true)]
async fn failed_refresh_is_shared_without_fallback_or_repeated_subprocesses() {
    let cache = cache(aws_grant("secret-token", Duration::from_secs(300)));
    tokio::time::advance(Duration::from_secs(271)).await;
    let refreshes = Arc::new(AtomicUsize::new(0));
    let mut readers = JoinSet::new();
    let expected_failure = Error::ReadCredentialsRequestFailed {
        details: databricks_cli::DatabricksErrorDetails {
            message: Some("Access refused.".into()),
            source_kind: Some("permission_denied"),
            message_truncated: false,
        },
    };
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        let refreshes = Arc::clone(&refreshes);
        let failure = expected_failure.clone();
        readers.spawn(async move {
            cache
                .get_or_refresh(|| async {
                    refreshes.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    Err(failure)
                })
                .await
                .err()
        });
    }
    while let Some(result) = readers.join_next().await {
        assert_eq!(result.unwrap(), Some(expected_failure.clone()));
    }
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache.refresh_failure().await,
        Some(expected_failure.clone())
    );
    let DeltaStorageCredentialProvider::Aws(provider) = cache.credential_provider() else {
        panic!("expected AWS provider");
    };
    let error = provider.get_credential().await.unwrap_err();
    use std::error::Error as _;
    assert_eq!(
        error.source().unwrap().downcast_ref::<Error>(),
        Some(&expected_failure)
    );
    let diagnostics = format!("{provider:?} {error} {error:?}");
    assert!(!diagnostics.contains("secret"), "{diagnostics}");
}

#[tokio::test(start_paused = true)]
async fn expired_replacements_and_changed_azure_credential_kinds_are_rejected() {
    let cache = cache(aws_grant("initial", Duration::from_secs(1)));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        cache
            .get_or_refresh(|| async { Ok(aws_grant("expired", Duration::ZERO)) })
            .await
            .err(),
        Some(Error::CredentialsExpired)
    );

    for (initial, replacement) in [
        (
            AzureCredential::SASToken(vec![("sig".into(), "secret".into())]),
            AzureCredential::BearerToken("secret".into()),
        ),
        (
            AzureCredential::BearerToken("secret".into()),
            AzureCredential::SASToken(vec![("sig".into(), "secret".into())]),
        ),
    ] {
        let cache = TableCredentialCache::new(
            "profile".into(),
            "table-id".into(),
            TableStorageLocation::parse("abfss://container@account.dfs.core.windows.net/table")
                .unwrap(),
            TemporaryTableCredentials {
                credentials: CloudCredentials::Azure(Arc::new(initial)),
                expires_at: SystemTime::now() + Duration::from_secs(1),
            },
        )
        .unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            cache
                .get_or_refresh(|| async {
                    Ok(TemporaryTableCredentials {
                        credentials: CloudCredentials::Azure(Arc::new(replacement)),
                        expires_at: SystemTime::now() + Duration::from_secs(300),
                    })
                })
                .await
                .err(),
            Some(Error::UnsupportedDirectRead)
        );
        assert_eq!(
            cache.refresh_failure().await,
            Some(Error::UnsupportedDirectRead)
        );
    }
}
