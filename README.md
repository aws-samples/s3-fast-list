# Amazon S3 fast list
Concurrently list Amazon S3 bucket with ListObjectsV2 API.

For more information about Amazon S3 ListObjectsV2 API, please visit:
https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html

## How to build
### Install Rust
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
### Build binary
inside of project folder, run:
```
cargo build --release
```
you will find binary at `target/release`

## How to use
### CLI
```
Usage: s3-fast-list [OPTIONS] <COMMAND>

Commands:
  list  fast list and export results
  diff  bi-dir fast list and diff results
  help  Print this message or the help of the given subcommand(s)

Options:
  -p, --prefix <PREFIX>            prefix to start with [default: /]
  -t, --threads <THREADS>          worker threads for runtime [default: 10]
  -c, --concurrency <CONCURRENCY>  max concurrency tasks for list operation [default: 100]
  -k, --ks-file <KS_FILE>          input key space hints file [default: {region}_{bucket}_ks_hints.input]
  -f, --filter <FILTER>            object filter expresion
  -l, --log                        log to file [default: fastlist_{datetime}.log]
      --endpoint <ENDPOINT>        custom S3 endpoint URL
      --force-path-style           force path-style addressing (default when using --endpoint)
  -h, --help                       Print help
  -V, --version                    Print version
```

### Full list of Amazon S3 buket inventory
Normally, to get a full list of objects from Amazon S3 bucket, your will have 2 options:

#### Non-realtime

[Amazon S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html) generate the inventory on a daily or weekly basis.

#### Near-realtime

[Listing object programmatically](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html) with AWS SDKs or AWS CLI will underlying sequentially invoke ListObjectsV2 API until all objects are enumerated.

#### List "GIANT" bucket

Above methods work for most use cases.

But if you have a million+ objects bucket and you need to get full objects list job done in a very short time span, you should consider invoke ListObjectsV2 API in parallel to shorten end-to-end time cost.

As long as supplied with a pre-segmented prefix (-k ks_hints.input) input file, this tools can concurrently invoke ListObjectsV2 API to shorten overall list time to get a full list of objects inventory.

### Custom S3 Endpoint

To use s3-fast-list with an S3-compatible object storage (LocalStack, Ceph, etc.), specify the `--endpoint` argument, set the `AWS_ENDPOINT_URL` environment variable, or set `endpoint_url` in an AWS profile and set the `AWS_PROFILE` environment variable.

When using a custom endpoint, path-style addressing is automatically enabled. You can explicitly control this behavior with the `--force-path-style` option.

```
s3-fast-list list --bucket my-bucket --endpoint https://s3.example.com
```

or

```
export AWS_ENDPOINT_URL=https://s3.example.com
s3-fast-list list --bucket my-bucket
```

### Path-Style Addressing

By default, when using a custom endpoint, path-style addressing is enabled. For some S3-compatible storage systems, you may need to explicitly set this option:

```
s3-fast-list list --bucket my-bucket --force-path-style
```

This changes the request URLs from virtual-hosted style (`https://my-bucket.s3.example.com/key`) to path style (`https://s3.example.com/my-bucket/key`).

### List mode
```
s3-fast-list list - fast list and export results
```
To fast list a single bucket and export all retrieved object metadata to output parquet file.

```
s3-fast-list diff - bi-dir fast list and diff results
```
To fast list a pair of buckets in parallel, compare object metadata of same object key based on "Size" and "Etag", export all retrieved object metadata with difference flag.

### Filter

For some reasons, when you want to apply some conditions to filter out objects found by it's metadata properties, you can apply a filter.

#### Filter syntax
Filter is a string of expression of condition.

Allowed variable: `SOURCE` and `TARGET` (`TARGET` only for diff mode).

Allowed property: `size` and `last_modified` in integer.

Examples:
```
# for list mode
# object size great than 1000 bytes and it's last modified time great equal to January 1, 2024 00:00:00 UTC
list --filter "SOURCE.size > 1000 && SOURCE.last_modified >= 1704038400"

# for diff mode
# object size from SOURCE bucket great than 1000 bytes and it's last modified time great equals to January 1, 2024 00:00:00 UTC
# or object size from TARGET bucket equals to 100000 bytes.
diff --filter "(SOURCE.size > 1000 && SOURCE.last_modified >= 1704038400) || TARGET.size == 100000"
```

#### Differences between modes
in `list` mode, filter apples to all objects.

in `diff` mode, filter apples for objects seen on both `SOURCE` and `TARGET` side (`DiffFlag` in `3`).

### Output
#### Object metadata (parquet file)

The metadata of all objects are exported to a parquet file at the end of run with following schema:

| Field | DataType |
| ----- | -------- |
| Key | Utf8 |
| Size | UInt64 |
| LastModified | UInt64 |
| ETag | Utf8 |
| DiffFlag | UIint8 |

in `list` mode, all `DiffFlag` marked in `1`

in `diff` mode, enum value description of `DiffFlag` field:
```
0 - Object seen on BOTH side, Size and ETag are EQUAL (all EQUAL objects will not be export in diff mode)
1 - Object seen ONLY at SOURCE bucket
2 - Object seen ONLY at TARGET bucket
3 - Object seen on BOTH side, Size or ETag is not EQUAL (use metadata of object from SOURCE side during export)
```

#### Prefix distribution (ks file)

A prefix distribution csv file with naming pattern `[{region}_]{bucket}_{datetime}.ks` is exported at the end of each run. The region prefix is included if a region was provided.

Inside of ks file, each line indicate the number of objects under that prefix with assumed use delimit of "/".

The following shows example contents of a ks file:
```
"Europe/France/Nouvelle-Aquitaine/Bordeaux","20"
"North America/Canada/Quebec/Montreal","1"
"North America/USA/Washington/Bellevue","13"
"North America/USA/Washington/Seattle","65"
```

There are 2 ways currently to generate ks file:

1. Every time your execute `s3-fast-list list | diff`, ks file will be dump by default in the name of `{region}_{bucket}_{datetime}.ks`.
2. With `ks-tool inventory -r {region} -m s3://{location_of_your_s3_inventory}.manifest.json -c {concurrency}`, you can generated ks file from you S3 inventory report in CVS format.

### Prepare your ks hints

Based on exported prefix distribution ks file, you could split your prefix into segments for parallel list.

A typical ks hints input file looks like:
```
North America/Canada/Quebec/Montreal
North America/USA/Washington/Seattle
```

Above ks hints will split all prefix into 3 segments:
1. [`""` VERY BEGIN to `North America/Canada/Quebec/Montreal`)
2. [`North America/Canada/Quebec/Montreal` to `North America/USA/Washington/Seattle`]
3. [`North America/USA/Washington/Seattle` to `""` VERY LAST]

Since the nature of ["List results are always returned in UTF-8 binary order"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html), ALL objects will be covered during concurrent listing.

Use [ks-tool](ks-tool) to split your ks to target count of prefix segments
```
ks-tool split -k {region}_{bucket}_{datetime}.ks -c {num of splits} -o {region}_{bucket}_ks_hints.input
```

## Performance test

A bucket with 100 million objects used as benchmark baseline

| Concurrency | Start Time | End Time | Duration(s) |
| ----------- | ---------- | -------- | -------- |
| 1 | 2024-01-01T05:06:38Z | 2024-01-01T07:23:32Z | 8214 |
| 10 | 2024-01-01T09:23:14Z | 2024-01-01T09:38:38Z | 924 |
| 100 | 2024-01-01T09:47:50Z | 2024-01-01T09:49:32Z | 102 |
| 1000 | 2024-01-01T10:17:59Z | 2024-01-01T10:18:31Z | 32 |

- All tests running on a m6i.8xlarge Amazon EC2 instance
- Duration stands for overall list time only, metadata export time is not included.

## Blogs

* [五行俱下 – 如何在短时间里遍历 Amazon S3 亿级对象桶（原理篇）](https://aws.amazon.com/cn/blogs/china/how-to-traverse-amazon-s3-billion-object-buckets-in-a-short-time-principle/)
* [一骑绝尘 – 如何在短时间里遍历 Amazon S3 亿级对象桶（工具篇）](https://aws.amazon.com/cn/blogs/china/how-to-traverse-amazon-s3-billion-object-buckets-in-a-short-time-tools/)

## Roadmap
- [ ] ~~Provide tools to generate ks hints~~
- [ ] ~~Generate ks hints from Amazon S3 Inventory~~
- [ ] Add support for directory buckets (Amazon S3 Express One Zone), see: https://github.com/awslabs/aws-sdk-rust/issues/992
- [ ] ~~Rule based metadata comparasion for diff mode~~

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

