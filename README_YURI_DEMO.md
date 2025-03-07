# README YURI
## To build 
- install c++ compiler, cmake, (optionally, but recommended: ninja, ccache)
- install vcpkg and configure, see https://github.com/duckdb/extension-template?tab=readme-ov-file#managing-dependencies
- run `make` or `make debug`

## To test this demo

Start the CLI:
```
./build/debug/duckdb
```


Then open duckdb and add a secret detailing how to assume the role with sts.
```SQL
CREATE SECRET my_secret (
    TYPE S3,
    PROVIDER credential_chain,
    CHAIN 'sts',
    ASSUME_ROLE_ARN 'arn:aws:iam::<account_id>:role/<desired_role>',
    REGION '<region>'
);
```

Now attach the Glue catalog:
```SQL
attach '<account_id>:s3tablescatalog:<bucket>' as my_datalake (
    TYPE ICEBERG,
    ENDPOINT_TYPE 'GLUE'
);
```

Fire away!

```SQL
SHOW ALL TABLES;
SELECT count(*) FROM my_datalake.myblognamespace.lineitem;
SELECT * FROM my_datalake.myblognamespace.lineitem LIMIT 10;
```







