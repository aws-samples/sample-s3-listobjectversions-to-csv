# Listing S3 Object Versions to CSV

A robust Python script for exporting a list of S3 object versions, in CSV format, at scale.

![screenshot](screenshots/screenshot1.png)
## Features

* **All Versions**: Export every version of every object key, not just the current version, including [delete markers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html)
    * If [Amazon S3 Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) is disabled, there will only be one version of each key, with a ‘null’ version ID, and no delete markers
* **Prefix support**: Export only a specific [prefix](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html), without scanning the entire bucket
* **Offline filtering and analysis**: Create multiple manifests from one export, without repeated API calls. Analyze for size or storage class distribution
* **Authentication token refresh:** Automatically recover on session token expiry
* **Resume capability:** Automatically resumes from checkpoint (in case of disconnects or other errors)
* **Output format control:** Choose to remove CSV column headings, or restrict columns to those expected by [S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops.html).
* **Memory-efficient:** Streaming output to CSV
* **Flexible authentication:** Supports all AWS credential methods

## Performance

* 3 million object versions → 432 MB CSV in 15 minutes (on a [t2.micro instance](https://aws.amazon.com/ec2/instance-types/t2/))
* 60 million object versions → 8 GB CSV in 5.5 hours

**Note**: S3 [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) operations must be sequential in order to ensure all objects are included, and this restricts performance. If desired, you could run multiple instances of this script in parallel against different prefixes (creating separate CSVs for each).
If you are working at 100+ million object scale, or want a regular inventory of your objects, [S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html) is likely a better choice. If you are working only with current object versions, S3 Batch Operations has a manifest generator - see **Generating a manifest automatically** in the [S3 Batch Operations documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-create-job.html).

## Usage

#### Standard output

```
python3 s3_listobjectversions_to_csv.py --bucket my-bucket
```

Outputs a file in the local directory named `s3_object_versions.csv`. Specify output file with `--output /path/filename.csv`

#### Specific prefix only

```
python s3_listobjectversions_to_csv.py --bucket my-bucket --prefix logs/2024/
```

#### Skip bucket versioning check

By default the script will check whether S3 Versioning is enabled. This requires permission to use the `s3:GetBucketVersioning` API on the bucket. To skip this check, use `--skipversioningcheck`

#### Specify an AWS profile

The script uses AWS CLI credentials. If you want to specify a particular profile, add `--profile my-profile` 

#### Disable resume

By default, re-running the previous command will cause the script to resume from the last checkpoint, appending the output file. To disable the checkpointing and resume functionality (overwriting any existing CSV), add `--noresume`

#### For all options and more detail

```
python s3_listobjectversions_to_csv.py -help
```


## Output Formats

#### **Standard** (default)

```
bucket_name,key_name,version_id,is_latest,delete_marker,size,last_modified,storage_class,e_tag
```

#### **No CSV Headers** (`--nocsvheaders`)

Doesn’t include a header row in the CSV.

#### **Batch Operations Manifest Compatible** (`--bopsmanifestcompatible`) 

Outputs only the columns that [S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops.html) expects. Automatically includes `--nocsvheaders`

```
bucket_name,key_name,version_id
```

#### **No URL-encoding** (`--nourlencoding`)

Prevents URL-encoding of object key names, for example replacing ` ` with `+` . Note that if you have commas in your object names, this will interfere with subsequent processing of the CSV output. URL-encoding is always enabled when `--bopsmanifestcompatible`  is enabled, as S3 Batch Operations requires URL-encoded keys.


## Example use cases for filtering

#### Create an S3 Batch Operations compatible manifest of noncurrent versions to delete

```
awk -F',' '$4=="False" && $5=="False"' s3_object_versions.csv | cut -d',' -f1-3 > old_versions.csv
```

#### Create a S3 Batch Operations manifest of delete markers to delete

```
awk -F',' '$5=="True"' s3_object_versions.csv | cut -d',' -f1-3 > delete_markers.csv
```

#### Create a S3 Batch Operations manifest of older objects to archive

```
awk -F',' 'substr($7,1,4) < "2023"' s3_object_versions.csv | cut -d',' -f1-3 > archive.csv
```

#### Current versions that are in the STANDARD storage class

```
awk -F',' '$4=="True" && $8=="STANDARD"' s3_object_versions.csv | cut -d',' -f1-3 > standard_current.csv
```

#### Filter for only current verions, excluding delete markers
```
awk -F',' 'NR==1 || ($4=="True" && $5!="True")' s3_object_versions.csv > ./current_versions.csv
```

#### Filter for only noncurrent verions
```
awk -F',' 'NR==1 || $4=="False"' s3_object_versions.csv > ./noncurrent_versions.csv
```

#### Filter for only objects <128 KiB
```
awk -F',' 'NR==1 || $6<131072' ./s3_object_versions.csv > ./small_objects.csv
```

#### Filter for only objects >=128 KiB
```
awk -F',' 'NR==1 || $6>131071' ./s3_object_versions.csv > ./larger_objects.csv
```

#### Analyze total and average object sizes

```
awk -F',' 'NR>1 && $5!="True" {c++; s+=$6; if($6<131072){sc++; ss+=$6}else{lc++; ls+=$6}} END {if(c==0){print "No objects found"}else{print "Category|Count/Percentage|Average Size|Total Size"; print "---|---|---|---"; printf "Total|%d objects|%.3g %s|%.3g %s\n", c, s/c>=1099511627776?s/c/1099511627776:(s/c>=1073741824?s/c/1073741824:(s/c>=1048576?s/c/1048576:s/c/1024)), s/c>=1099511627776?"TiB":(s/c>=1073741824?"GiB":(s/c>=1048576?"MiB":"KiB")), s>=1099511627776?s/1099511627776:(s>=1073741824?s/1073741824:(s>=1048576?s/1048576:s/1024)), s>=1099511627776?"TiB":(s>=1073741824?"GiB":(s>=1048576?"MiB":"KiB")); sp=s>0?ss*100/s:0; printf "Small objects|%.1f%% < 128KiB|%.3g %s|%.1f%% (%.3g %s)\n", sc*100/c, sc?(ss/sc>=1099511627776?ss/sc/1099511627776:(ss/sc>=1073741824?ss/sc/1073741824:(ss/sc>=1048576?ss/sc/1048576:ss/sc/1024))):0, sc?(ss/sc>=1099511627776?"TiB":(ss/sc>=1073741824?"GiB":(ss/sc>=1048576?"MiB":"KiB"))):"KiB", sp, ss>=1099511627776?ss/1099511627776:(ss>=1073741824?ss/1073741824:(ss>=1048576?ss/1048576:ss/1024)), ss>=1099511627776?"TiB":(ss>=1073741824?"GiB":(ss>=1048576?"MiB":"KiB")); lp=s>0?ls*100/s:0; printf "Larger objects|%.1f%% >= 128KiB|%.3g %s|%.1f%% (%.3g %s)\n", lc*100/c, lc?(ls/lc>=1099511627776?ls/lc/1099511627776:(ls/lc>=1073741824?ls/lc/1073741824:(ls/lc>=1048576?ls/lc/1048576:ls/lc/1024))):0, lc?(ls/lc>=1099511627776?"TiB":(ls/lc>=1073741824?"GiB":(ls/lc>=1048576?"MiB":"KiB"))):"KiB", lp, ls>=1099511627776?ls/1099511627776:(ls>=1073741824?ls/1073741824:(ls>=1048576?ls/1048576:ls/1024)), ls>=1099511627776?"TiB":(ls>=1073741824?"GiB":(ls>=1048576?"MiB":"KiB"))}}' ./s3_object_versions.csv | column -t -s'|'
```
*Tip: This can be run against a filtered output for current or noncurrent object versions.*

Example output:
```
Category        Count/Percentage  Average Size  Total Size
---             ---               ---           ---
Total           58210 objects     71.5 MiB      3.97 TiB
Small objects   98.6% < 128KiB    0.00977 KiB   0.0% (561 KiB)
Larger objects  1.4% >= 128KiB    5.08 GiB      100.0% (3.97 TiB)
```

#### Analyze storage class usage
```
awk -F',' 'NR>1 && $5!="True" {c++; s+=$6; sc[$8]++; ss[$8]+=$6} END {if(c==0){print "No objects found"}else{printf "Storage Class|Count/Percentage|Average Size|Total Size\n---|---|---|---\nTotal|%d objects|%.3g %s|%.3g %s\n", c, s/c>=1099511627776?s/c/1099511627776:s/c>=1073741824?s/c/1073741824:s/c>=1048576?s/c/1048576:s/c/1024, s/c>=1099511627776?"TiB":s/c>=1073741824?"GiB":s/c>=1048576?"MiB":"KiB", s>=1099511627776?s/1099511627776:s>=1073741824?s/1073741824:s>=1048576?s/1048576:s/1024, s>=1099511627776?"TiB":s>=1073741824?"GiB":s>=1048576?"MiB":"KiB"; for(cl in sc){name=cl; gsub("REDUCED_REDUNDANCY","Reduced Redundancy",name);  gsub("DEEP_ARCHIVE","Glacier Deep Archive",name); gsub("GLACIER_IR","Glacier Instant Retrieval",name); gsub("GLACIER","Glacier Flexible Retrieval",name); gsub("INTELLIGENT_TIERING","Intelligent-Tiering",name); gsub("ONEZONE_IA","One Zone-IA",name); gsub("STANDARD_IA","Standard-IA",name); gsub("STANDARD","Standard",name); printf "%s|%.1f%% (%d)|%.3g %s|%.3g %s\n", name, sc[cl]*100/c, sc[cl], ss[cl]/sc[cl]>=1099511627776?ss[cl]/sc[cl]/1099511627776:ss[cl]/sc[cl]>=1073741824?ss[cl]/sc[cl]/1073741824:ss[cl]/sc[cl]>=1048576?ss[cl]/sc[cl]/1048576:ss[cl]/sc[cl]/1024, ss[cl]/sc[cl]>=1099511627776?"TiB":ss[cl]/sc[cl]>=1073741824?"GiB":ss[cl]/sc[cl]>=1048576?"MiB":"KiB", ss[cl]>=1099511627776?ss[cl]/1099511627776:ss[cl]>=1073741824?ss[cl]/1073741824:ss[cl]>=1048576?ss[cl]/1048576:ss[cl]/1024, ss[cl]>=1099511627776?"TiB":ss[cl]>=1073741824?"GiB":ss[cl]>=1048576?"MiB":"KiB"}}}' ./s3_object_versions.csv | column -t -s'|'
```
Example output:
```
Storage Class               Count/Percentage  Average Size  Total Size
---                         ---               ---           ---
Total                       58210 objects     71.5 MiB      3.97 TiB
Standard                    45.5% (26510)     19.6 MiB      508 GiB
Intelligent-Tiering         1.2% (700)        5.08 GiB      3.47 TiB
Glacier Deep Archive        27.5% (16000)     0.00977 KiB   156 KiB
Glacier Flexible Retrieval  25.8% (15000)     0.00977 KiB   146 KiB
```
*Tip: This can be run against a filtered output < or >= 128 KiB objects.*

## Explanation of resume functionality

#### **Checkpoint creation**

* Saves progress every 20 batches (20,000 items)
* Saves on Ctrl+C cancellation
* Saves on errors before exit
* Checkpoint filename includes checksum of job parameters: `outputfile_bucket_checksum.json`

#### **What's saved**

* Current batch number and position markers
* Total items processed so far
* List of processed object keys+versions (for deduplication)
* Timestamp and job parameters

#### **Resume process**

* The resume is automatic - just run the same command again and the script will pick up where it left off.
* Looks for existing checkpoint file with matching checksum
* Rejects checkpoints older than 24 hours
* Validates CSV has not been updated externally
* Prompts to overwrite CSV if resume fails (mismatched parameters, old checkpoint, etc.)
* Loads processed keys to avoid duplicates
* Continues from last saved position using S3 pagination markers
* Tracks unique `key_name-version_id` combinations
* Skips already processed items when resuming
* Appends new data to existing CSV file
* Removes checkpoint file on successful completion




## Cost

The script lists the default and maximum of 1000 objects per request. Listing 100 million object versions in the Standard or Intelligent-Tiering [storage classes](https://aws.amazon.com/s3/storage-classes/) (using 100,000 requests) costs $0.50 at time of writing in us-east-1. For details, see https://aws.amazon.com/s3/pricing .


## Requirements

* Python 3.6+
* [Boto3](https://boto3.amazonaws.com/)- install with `pip install boto3`
* Minimum permission: `s3:ListBucketVersions`
    * `s3:GetBucketVersioning` is also required, unless using the option `--skipversioningcheck`
* An S3 [general purpose bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html). The script does not work with S3 [directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html) used with the [S3 Express One Zone storage class](https://aws.amazon.com/s3/storage-classes/express-one-zone/), or [S3 Table buckets](https://aws.amazon.com/s3/features/tables/).



## Security note

Some scanners may incorrectly flag this script with [CWE-78](https://cwe.mitre.org/data/definitions/78.html) due to its use of a subprocess call when refreshing credentials. `Shell=False` is set explicitly to mitigate this, as well as input and profile name validation.
