# Snowflake Connector
Under development.

# Usage
Right now, only [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) is supported.

Add your public and private key under `environment_variables/local` folder (you will have to create the `local` folder). Make sure your private key is named `rsa_key.p8` and your public key is `rsa_key.pub`.

You can change the text files in `environment_variables/snowflake_private_key_path.txt` and `environment_variables/snowflake_public_key_path.txt` to change where the application looks for the keys.
