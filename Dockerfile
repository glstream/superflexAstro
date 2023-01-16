FROM quay.io/astronomer/astro-runtime:7.1.0



ENV AIRFLOW_SECRETS_BACKEND=airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
ENV AIRFLOW_SECRETS_BACKEND_KWARGS='{"connections_prefix": "airflow-connections", "vault_url": "${KEY_VAULT_NAME}"}'


