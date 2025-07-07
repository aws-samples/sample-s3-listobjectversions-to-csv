#!/usr/bin/env python3
"""
Amazon S3 Object Versions to CSV Export Script

This script uses boto3 to list all object versions in a single S3 bucket
and exports the data to a CSV file with streaming processing.
Supports resume from checkpoint in case of client or credentials error.

In testing, listed 3M objects into a 432 MiB CSV in 15 minutes on a t2.micro instance, and 60M objects into a 8 GiB CSV in 5.5 hours.


License: 
MIT No Attribution

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import csv
import sys
import argparse
import threading
import json
import os
from typing import List, Dict, Any, Optional, Set
from queue import Queue
import time

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
except ImportError:
    print("Error: boto3 module not found. Install boto3 with: pip3 install boto3")
    sys.exit(1)

from datetime import datetime, timedelta
import hashlib
import urllib.parse


class S3VersionsExporter:
    def __init__(self, profile_name: Optional[str] = None, region_name: Optional[str] = None):
        self.total_processed = 0
        self.error_count = 0
        self.lock = threading.Lock()
        self.write_queue = Queue(maxsize=10)
        self.writer_thread = None
        self.stop_writer = False
        
        # Validate inputs
        if profile_name:
            import re
            if not re.match(r'^[a-zA-Z0-9_-]+$', profile_name):
                raise ValueError("Invalid profile name - contains unsafe characters")
        if region_name:
            if not re.match(r'^[a-z0-9-]+$', region_name):
                raise ValueError("Invalid region name format")
                
        self.profile_name = profile_name
        self.region_name = region_name
        self.processed_keys = set()  # Track processed object keys+versions to avoid duplicates
        self.resume_state_file = None
        self.current_batch_num = 0
        self.checkpoint_printed = False
        
        self._initialize_client()

    def _initialize_client(self):
        """Initialize or re-initialize the boto3 S3 client."""
        try:
            if self.profile_name:
                session = boto3.Session(profile_name=self.profile_name)
                self.s3_client = session.client('s3', region_name=self.region_name)
                sts_client = session.client('sts')
            else:
                self.s3_client = boto3.client('s3', region_name=self.region_name)
                sts_client = boto3.client('sts')
                
            # Test credentials using STS GetCallerIdentity (requires no special permissions)
            sts_client.get_caller_identity()
            print("✓ AWS credentials verified successfully")
            
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error: AWS credentials not found or incomplete: {e}")
            print("Please configure your AWS credentials using:")
            print("  - AWS CLI: aws configure")
            print("  - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            print("  - IAM roles (if running on EC2)")
            sys.exit(1)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code in ['ExpiredToken', 'InvalidToken', 'TokenRefreshRequired']:
                # Don't exit here - let the caller handle the refresh
                raise
            else:
                print(f"Error: AWS client error: {e}")
                sys.exit(1)
        except Exception as e:
            print(f"Error initializing AWS client: {e}")
            sys.exit(1)

    def _auto_refresh_sso_credentials(self) -> bool:
        """Attempt to automatically refresh SSO credentials."""
        if not self.profile_name:
            return False
            
        # Validate profile name to prevent command injection
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', self.profile_name):
            print("Invalid profile name - contains unsafe characters")
            return False
            
        try:
            import subprocess
            import shutil
            print("Attempting to automatically refresh SSO credentials...")
            
            # Find full path to aws executable to avoid partial path security issue
            aws_path = shutil.which('aws')
            if not aws_path:
                print("AWS CLI not found in PATH - cannot auto-refresh SSO")
                return False
            
            # Use subprocess.run with full executable path and explicit arguments list
            result = subprocess.run(
                [aws_path, 'sso', 'login', '--profile', self.profile_name],
                capture_output=True,
                text=True,
                timeout=60,
                shell=False
            )
            
            if result.returncode == 0:
                print("✓ SSO credentials refreshed successfully")
                return True
            else:
                print(f"SSO refresh failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("SSO refresh timed out (requires manual login)")
            return False
        except FileNotFoundError:
            print("AWS CLI not found - cannot auto-refresh SSO")
            return False
        except Exception as e:
            print(f"SSO auto-refresh failed: {e}")
            return False

    def _generate_resume_state_filename(self, bucket_name: str, prefix: str = None, output_file: str = None) -> str:
        """Generate a unique resume state filename based on job parameters."""
        # Create a hash of the job parameters to ensure uniqueness
        job_params = f"{bucket_name}:{prefix or ''}:{output_file or ''}"
        job_hash = hashlib.md5(job_params.encode(), usedforsecurity=False).hexdigest()[:8]
        
        # Generate checkpoint filename based on output file
        output_path = output_file or 's3_object_versions.csv'
        base_name = os.path.splitext(output_path)[0]  # Remove extension
        output_dir = os.path.dirname(os.path.abspath(output_path))
        
        checkpoint_filename = f"{os.path.basename(base_name)}_{bucket_name}_{job_hash}.json"
        return os.path.join(output_dir, checkpoint_filename)

    def _save_resume_state(self, bucket_name: str, prefix: str, next_key_marker: str, 
                          next_version_marker: str, batch_num: int, total_items: int, output_file: str = None):
        """Save current progress state for resume capability."""
        if not self.resume_state_file:
            print("Warning: resume_state_file is None, cannot save checkpoint")
            return
            
        state = {
            'bucket_name': bucket_name,
            'prefix': prefix,
            'next_key_marker': next_key_marker,
            'next_version_marker': next_version_marker,
            'batch_num': batch_num,
            'total_items': total_items,
            'timestamp': datetime.now().isoformat(),
            'original_start_time': getattr(self, 'original_start_time', time.time()),
            'cumulative_runtime': getattr(self, 'cumulative_runtime', 0) + (time.time() - self.session_start_time),
            'csv_mtime': os.path.getmtime(output_file) if os.path.exists(output_file) else None,
            'processed_keys': list(self.processed_keys)  # Convert set to list for JSON
        }
        
        try:
            # Create file with secure permissions (owner read/write only)
            with open(self.resume_state_file, 'w') as f:
                json.dump(state, f, indent=2)
            os.chmod(self.resume_state_file, 0o600)
            if not self.checkpoint_printed:
                print(f"Checkpoint saved: {self.resume_state_file}")
                self.checkpoint_printed = True
        except Exception as e:
            print(f"Warning: Could not save resume state: {e}")

    def _load_resume_state(self, bucket_name: str, prefix: str, output_file: str = None) -> Dict[str, Any]:
        """Load previous progress state for resume capability."""
        if not self.resume_state_file or not os.path.exists(self.resume_state_file):
            return {'error': 'checkpoint file does not exist'}
            
        try:
            with open(self.resume_state_file, 'r') as f:
                state = json.load(f)
                
            # Validate state matches current job
            if state.get('bucket_name') != bucket_name:
                return {'error': f'bucket mismatch (checkpoint: {state.get("bucket_name")}, current: {bucket_name})'}
            if state.get('prefix') != prefix:
                return {'error': f'prefix mismatch (checkpoint: {state.get("prefix")}, current: {prefix})'}
                
            # Check if checkpoint is more than 24 hours old
            if state.get('timestamp'):
                try:
                    checkpoint_time = datetime.fromisoformat(state['timestamp'])
                    if datetime.now() - checkpoint_time > timedelta(hours=24):
                        return {'error': 'checkpoint is more than 24 hours old'}
                except (ValueError, TypeError):
                    return {'error': 'invalid checkpoint timestamp'}
                
            # Load processed keys to avoid duplicates
            self.processed_keys = set(state.get('processed_keys', []))
            
            # Load original start time and cumulative runtime
            self.original_start_time = state.get('original_start_time', time.time())
            self.cumulative_runtime = state.get('cumulative_runtime', 0)
            
            # Check if CSV file was modified externally
            if state.get('csv_mtime') and os.path.exists(output_file):
                current_mtime = os.path.getmtime(output_file)
                if abs(current_mtime - state['csv_mtime']) > 10:  # Allow 10 second tolerance
                    return {'error': 'CSV file was modified externally since last checkpoint'}
            
            print(f"Found resume state from {state.get('timestamp', 'unknown time')}")
            print(f"Previous progress: {state.get('total_items', 0):,} items processed")
            
            return state
            
        except Exception as e:
            return {'error': f'corrupted checkpoint file ({str(e)})'}

    def _cleanup_resume_state(self):
        """Clean up resume state file after successful completion."""
        if self.resume_state_file and os.path.exists(self.resume_state_file):
            try:
                os.remove(self.resume_state_file)
                print("✓ Resume state file cleaned up")
            except Exception as e:
                print(f"Warning: Could not clean up resume state file: {e}")

    def _check_existing_csv_for_resume(self, output_file: str, silent: bool = False, allow_skip: bool = False) -> int:
        """Check existing CSV file and count existing records to avoid duplicates."""
        if not os.path.exists(output_file):
            return 0
            
        try:
            existing_count = 0
            with open(output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Create unique key for deduplication
                    key_version = f"{row.get('key_name', '')}-{row.get('version_id', '')}"
                    self.processed_keys.add(key_version)
                    existing_count += 1
                    
            if not silent:
                print(f"Found existing CSV with {existing_count:,} records")
            return existing_count
            
        except Exception as e:
            print(f"Warning: Could not read existing CSV file: {e}")
            return 0

    def _refresh_credentials_and_retry(self, max_retries: int = 3) -> bool:
        """Attempt to refresh AWS credentials and reinitialize the client."""
        for attempt in range(max_retries):
            try:
                print(f"Attempting to refresh credentials (attempt {attempt + 1}/{max_retries})...")
                
                # Wait a bit before retrying
                time.sleep(2 ** attempt)  # Exponential backoff: 2, 4, 8 seconds
                
                # Force credential refresh by creating a completely new session and client
                # This is important because boto3 caches credentials internally
                if self.profile_name:
                    # For profiles, create a new session which will reload credentials
                    new_session = boto3.Session(profile_name=self.profile_name)
                    from botocore.config import Config
                    config = Config(
                        retries={'max_attempts': 3, 'mode': 'standard'},
                        connect_timeout=10,
                        read_timeout=30,
                        max_pool_connections=10
                    )
                    self.s3_client = new_session.client('s3', region_name=self.region_name, config=config)
                else:
                    # For default credentials, clear any cached credentials and create new client
                    # This forces boto3 to re-read from credential sources
                    boto3.DEFAULT_SESSION = None
                    from botocore.config import Config
                    config = Config(
                        retries={'max_attempts': 3, 'mode': 'standard'},
                        connect_timeout=10,
                        read_timeout=30,
                        max_pool_connections=10
                    )
                    self.s3_client = boto3.client('s3', region_name=self.region_name, config=config)
                
                # Test the new credentials
                if self.profile_name:
                    sts_client = new_session.client('sts')
                else:
                    sts_client = boto3.client('sts')
                sts_client.get_caller_identity()
                print(f"✓ Credential refresh attempt {attempt + 1} successful")
                return True
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                if error_code in ['ExpiredToken', 'InvalidToken', 'TokenRefreshRequired']:
                    print(f"Credential refresh attempt {attempt + 1} failed: credentials still expired")
                    if attempt == max_retries - 1:
                        print("All credential refresh attempts failed - credentials are still expired.")
                        print("\nThis usually means:")
                        if self.profile_name:
                            print(f"- Your AWS SSO session has expired. Run: aws sso login --profile {self.profile_name}")
                            print(f"- Or your assume role session has expired. Re-run your assume role command")
                        else:
                            print("- Your AWS credentials/session have expired")
                            print("- For SSO: Run 'aws sso login'")
                            print("- For assume role: Re-run your assume role command")
                            print("- For temporary credentials: Generate new ones")
                    continue
                else:
                    print(f"Credential refresh attempt {attempt + 1} failed with different error: {e}")
                    return False
            except Exception as e:
                print(f"Credential refresh attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    print("All credential refresh attempts failed.")
                    return False
        
        return False

    def csv_writer_worker(self, output_file: str, fieldnames: List[str], write_headers: bool = True, append_mode: bool = False):
        """Background thread worker for writing CSV data with sorting."""
        try:
            # Validate output file path
            if not self._validate_file_path(output_file):
                raise ValueError("Invalid output file path")
                
            # Create parent directories if they don't exist
            os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
                
            # Use append mode if resuming
            file_mode = 'a' if append_mode else 'w'
            
            with open(output_file, file_mode, newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Only write headers if not in append mode and headers are requested
                if write_headers and not append_mode:
                    writer.writeheader()
                
                sort_buffer = []
                
                while not self.stop_writer or not self.write_queue.empty():
                    try:
                        batch_start = time.time()
                        current_batch = []
                        
                        # Collect batch or timeout after 2 seconds
                        while len(current_batch) < 2000 and (time.time() - batch_start) < 2.0:
                            try:
                                item = self.write_queue.get(timeout=0.5)
                                if item is None:  # Sentinel value to stop
                                    self.stop_writer = True
                                    break
                                current_batch.append(item)
                                self.write_queue.task_done()
                            except Exception:
                                break
                        
                        sort_buffer.extend(current_batch)
                        
                        # Write buffer when it gets large enough or we're stopping
                        if len(sort_buffer) >= 4000 or (self.stop_writer and sort_buffer):
                            sort_buffer.sort(key=lambda x: x['key_name'])
                            
                            for row in sort_buffer:
                                writer.writerow(row)
                            
                            csvfile.flush()
                            
                            # Update counter but don't print here - let main thread handle progress
                            with self.lock:
                                self.total_processed += len(sort_buffer)
                            
                            sort_buffer = []
                    
                    except Exception as e:
                        print(f"Error in CSV writer: {e}")
                        with self.lock:
                            self.error_count += 1
                
                # Write any remaining items
                if sort_buffer:
                    sort_buffer.sort(key=lambda x: x['key_name'])
                    for row in sort_buffer:
                        writer.writerow(row)
                    with self.lock:
                        self.total_processed += len(sort_buffer)
                        
        except Exception as e:
            print(f"Fatal error in CSV writer: {e}")
            sys.exit(1)

    def process_batch(self, bucket_name: str, batch_num: int, prefix: str = None, 
                     key_marker: str = None, version_marker: str = None, bops_compatible: bool = False, url_encode: bool = True) -> Dict[str, Any]:
        """Process a single batch of object versions using boto3."""
        try:
            # Prepare parameters for list_object_versions
            params = {
                'Bucket': bucket_name,
                'MaxKeys': 1000  # boto3 default, can be up to 1000
            }
            
            if prefix:
                params['Prefix'] = prefix
            if key_marker:
                params['KeyMarker'] = key_marker
            if version_marker:
                params['VersionIdMarker'] = version_marker
            
            response = self.s3_client.list_object_versions(**params)
            batch_count = 0
            
            # Process versions (regular objects)
            for version in response.get('Versions', []):
                # Create unique key for deduplication
                key_version = f"{version['Key']}-{version['VersionId']}"
                
                # Skip if already processed (for resume capability)
                if key_version in self.processed_keys:
                    continue
                    
                # Add to processed set
                self.processed_keys.add(key_version)
                
                # Convert datetime to string if it's a datetime object
                last_modified = version['LastModified']
                if isinstance(last_modified, datetime):
                    last_modified = last_modified.isoformat()
                
                key_name = urllib.parse.quote_plus(version['Key'], safe='/', encoding='utf-8') if url_encode else version['Key']
                
                if bops_compatible:
                    row = {
                        'bucket_name': bucket_name,
                        'key_name': key_name,
                        'version_id': version['VersionId']
                    }
                else:
                    row = {
                        'bucket_name': bucket_name,
                        'key_name': key_name,
                        'version_id': version['VersionId'],
                        'is_latest': version['IsLatest'],
                        'delete_marker': False,
                        'size': version['Size'],
                        'last_modified': last_modified,
                        'storage_class': version.get('StorageClass', 'STANDARD'),
                        'e_tag': version.get('ETag', '').strip('"')
                    }
                self.write_queue.put(row)
                batch_count += 1
            
            # Process delete markers
            for delete_marker in response.get('DeleteMarkers', []):
                # Create unique key for deduplication
                key_version = f"{delete_marker['Key']}-{delete_marker['VersionId']}"
                
                # Skip if already processed (for resume capability)
                if key_version in self.processed_keys:
                    continue
                    
                # Add to processed set
                self.processed_keys.add(key_version)
                
                # Convert datetime to string if it's a datetime object
                last_modified = delete_marker['LastModified']
                if isinstance(last_modified, datetime):
                    last_modified = last_modified.isoformat()
                
                key_name = urllib.parse.quote_plus(delete_marker['Key'], safe='/', encoding='utf-8') if url_encode else delete_marker['Key']
                
                if bops_compatible:
                    row = {
                        'bucket_name': bucket_name,
                        'key_name': key_name,
                        'version_id': delete_marker['VersionId']
                    }
                else:
                    row = {
                        'bucket_name': bucket_name,
                        'key_name': key_name,
                        'version_id': delete_marker['VersionId'],
                        'is_latest': delete_marker['IsLatest'],
                        'delete_marker': True,
                        'size': 0,
                        'last_modified': last_modified,
                        'storage_class': 'STANDARD',
                        'e_tag': delete_marker.get('ETag', '').strip('"')
                    }
                self.write_queue.put(row)
                batch_count += 1
            
            is_truncated = response.get('IsTruncated', False)
            next_key = response.get('NextKeyMarker')
            next_version = response.get('NextVersionIdMarker')
            
            return {
                'batch_count': batch_count,
                'is_truncated': is_truncated,
                'next_key_marker': next_key,
                'next_version_marker': next_version,
                'error': None
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'NoSuchBucket':
                error_msg = f"Bucket '{bucket_name}' does not exist or you don't have access to it"
            elif error_code == 'AccessDenied':
                error_msg = f"Access denied to bucket '{bucket_name}'. Check your permissions."
            elif error_code == 'ExpiredToken':
                error_msg = f"AWS credentials have expired. Please refresh your credentials and retry."
            elif error_code == 'InvalidToken':
                error_msg = f"AWS credentials are invalid. Please check your credentials and retry."
            elif error_code == 'TokenRefreshRequired':
                error_msg = f"AWS token refresh required. Please refresh your credentials and retry."
            else:
                # Don't expose detailed AWS error messages that might contain sensitive info
                error_msg = f"AWS Error [{error_code}]: Access or configuration issue"
            
            with self.lock:
                self.error_count += 1
            
            return {
                'batch_count': 0,
                'is_truncated': False,
                'next_key_marker': None,
                'next_version_marker': None,
                'error': error_msg,
                'retry_after_refresh': error_code in ['ExpiredToken', 'InvalidToken', 'TokenRefreshRequired']
            }
            
        except Exception as e:
            with self.lock:
                self.error_count += 1
            return {
                'batch_count': 0,
                'is_truncated': False,
                'next_key_marker': None,
                'next_version_marker': None,
                'error': str(e),
                'retry_after_refresh': False
            }



    def check_bucket_versioning(self, bucket_name: str) -> None:
        """Check if bucket has versioning enabled and warn if not."""
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            status = response.get('Status', 'Disabled')
            
            if status == 'Suspended':
                print("⚠️  Warning: Bucket versioning is suspended. New objects will have 'null' Version ID.")
            elif status != 'Enabled':
                print("⚠️  Warning: Bucket versioning is disabled. All objects will have 'null' Version ID.")
            else:
                print("✓ Bucket versioning is enabled")
                
        except ClientError as e:
            print(f"Warning: Could not check bucket versioning status: {e}")
        except Exception as e:
            print(f"Warning: Could not check bucket versioning status: {e}")

    def _validate_bucket_name(self, bucket_name: str) -> bool:
        """Validate S3 bucket name format."""
        import re
        # Basic S3 bucket name validation
        if not re.match(r'^[a-z0-9.-]{3,63}$', bucket_name):
            return False
        if bucket_name.startswith('.') or bucket_name.endswith('.'):
            return False
        if '..' in bucket_name:
            return False
        return True
    
    def _validate_file_path(self, file_path: str) -> bool:
        """Validate file path to prevent directory traversal."""
        import os
        # Resolve path and check it doesn't escape current directory
        try:
            resolved_path = os.path.abspath(file_path)
            current_dir = os.path.abspath(os.getcwd())
            return resolved_path.startswith(current_dir)
        except (OSError, ValueError):
            return False

    def export_versions(self, bucket_name: str, prefix: str = None, output_file: str = None, 
                       skip_versioning_check: bool = False, write_headers: bool = True, enable_resume: bool = True, bops_compatible: bool = False, url_encode: bool = True) -> None:
        """Main export function with sequential processing and resume capability."""
        # Record start time
        start_time = time.time()
        self.session_start_time = start_time
        if not hasattr(self, 'original_start_time'):
            self.original_start_time = start_time
        if not hasattr(self, 'cumulative_runtime'):
            self.cumulative_runtime = 0
        
        # Apply default output file if none specified
        if not output_file:
            output_file = 's3_object_versions.csv'
            
        # Validate inputs
        if not self._validate_bucket_name(bucket_name):
            raise ValueError("Invalid bucket name format")
        if not self._validate_file_path(output_file):
            raise ValueError("Invalid output file path")
            
        if bops_compatible:
            fieldnames = ['bucket_name', 'key_name', 'version_id']
        else:
            fieldnames = [
                'bucket_name', 'key_name', 'version_id', 'is_latest',
                'delete_marker', 'size', 'last_modified', 'storage_class', 'e_tag'
            ]
        
        print(f"Starting export for bucket: {bucket_name}")
        if prefix:
            print(f"Using prefix: {prefix}")
        
        # Set up resume capability
        resume_state = {}
        append_mode = False
        existing_csv_count = 0
        
        if enable_resume:
            # Always set resume_state_file when resume is enabled
            self.resume_state_file = self._generate_resume_state_filename(bucket_name, prefix, output_file)
            
            # If output file doesn't exist, clean up any existing checkpoint but keep resume enabled for new checkpoints
            if not os.path.exists(output_file):
                if os.path.exists(self.resume_state_file):
                    try:
                        os.remove(self.resume_state_file)
                        print("Cleaned up stale checkpoint file")
                    except OSError as e:
                        print(f"Warning: Could not remove stale checkpoint: {e}")
            else:
                resume_state = self._load_resume_state(bucket_name, prefix, output_file)
                
                # Check for resume errors
                if 'error' in resume_state:
                    print(f"Cannot resume due to {resume_state['error']}")
                    if os.path.exists(output_file):
                        print(f"Found existing {os.path.basename(output_file)}")
                        response = input(f"Overwrite? (y/n): ")
                        if response.lower().startswith('y'):
                            existing_csv_count = 0  # Don't count rows since we're overwriting
                        if not response.lower().startswith('y'):
                            print("Operation cancelled")
                            sys.exit(0)
                        print("Will overwrite existing file")
                        self.processed_keys.clear()
                        existing_csv_count = 0  # Reset count since we're overwriting
                    # Keep resume enabled for saving checkpoints during this run
                    resume_state = {}
        
        if enable_resume and resume_state and 'error' not in resume_state:
            append_mode = True
            existing_csv_count = self._check_existing_csv_for_resume(output_file, silent=True)
            print(f"Resume mode enabled - will append to existing CSV")
        
        # Check bucket versioning status (optional) - skip if appending to existing data
        if not skip_versioning_check and not append_mode:
            self.check_bucket_versioning(bucket_name)
        elif skip_versioning_check:
            print("Skipping bucket versioning check")
        
        # Start CSV writer thread
        self.writer_thread = threading.Thread(
            target=self.csv_writer_worker,
            args=(output_file, fieldnames, write_headers, append_mode)
        )
        self.writer_thread.start()
        
        try:
            # Initialize counters and markers
            if resume_state:
                next_key_marker = resume_state.get('next_key_marker')
                next_version_marker = resume_state.get('next_version_marker')
                batch_num = resume_state.get('batch_num', 0)
                total_items = resume_state.get('total_items', 0)
                self.current_batch_num = batch_num
                print(f"Resuming from batch {batch_num + 1}")
                if next_key_marker:
                    print(f"Next key to process: {next_key_marker}")
            else:
                next_key_marker = None
                next_version_marker = None
                batch_num = 0
                total_items = existing_csv_count
                self.current_batch_num = 0
            
            last_progress_time = time.time()
            save_state_interval = 20  # Save state every 20 batches
            key_trim_interval = 100  # Trim processed keys every 100 batches
            
            while True:
                batch_num += 1
                self.current_batch_num = batch_num
                
                result = self.process_batch(
                    bucket_name, 
                    batch_num,
                    prefix, 
                    next_key_marker, 
                    next_version_marker,
                    bops_compatible,
                    url_encode
                )
                
                if result['error']:
                    print(f"Error in batch {batch_num}: {result['error']}")
                    
                    # Save state before attempting recovery
                    if enable_resume:
                        self._save_resume_state(bucket_name, prefix, next_key_marker, 
                                              next_version_marker, batch_num - 1, total_items, output_file)
                    
                    # If it's a credential expiration error, try to refresh and retry
                    if result.get('retry_after_refresh', False):
                        print("Attempting automatic credential refresh...")
                        
                        # First try automatic SSO refresh if using a profile
                        sso_refreshed = False
                        if self.profile_name:
                            sso_refreshed = self._auto_refresh_sso_credentials()
                        
                        # Try credential refresh (with new session)
                        if sso_refreshed or self._refresh_credentials_and_retry():
                            print(f"✓ Credentials refreshed successfully. Retrying batch {batch_num}...")
                            continue  # Retry the same batch with refreshed credentials
                        else:
                            print("Failed to refresh credentials automatically.")
                            print("\nManual steps to resolve:")
                            print("1. For AWS SSO: Run 'aws sso login --profile <profile-name>'")
                            print("2. For assume role: Re-run your assume role command")
                            print("3. For temporary credentials: Generate new credentials")
                            print("4. Then restart this script - it will resume from where it left off")
                            print(f"\nProgress: {total_items:,} items processed so far")
                            if enable_resume:
                                print(f"Resume state saved to: {self.resume_state_file}")
                    
                    # Save checkpoint before exiting on fatal error
                    if enable_resume:
                        self._save_resume_state(bucket_name, prefix, next_key_marker, 
                                              next_version_marker, batch_num - 1, total_items, output_file)
                    sys.exit(1)
                
                batch_count = result['batch_count']
                total_items += batch_count
                
                # Save resume state periodically
                if enable_resume and self.resume_state_file and batch_num % save_state_interval == 0:
                    self._save_resume_state(bucket_name, prefix, result.get('next_key_marker'), 
                                          result.get('next_version_marker'), batch_num, total_items, output_file)
                
                # Trim processed keys periodically to control memory usage
                if enable_resume:
                    current_count = len(self.processed_keys)
                    if current_count >= 200000:
                        self.processed_keys = set(list(self.processed_keys)[-10000:])
                        print(f"Trimmed checkpoint")
                
                # Show progress every 10 seconds
                current_time = time.time()
                if (current_time - last_progress_time) >= 10.0:
                    print(f"Total items processed: {total_items:,} (batch {batch_num})")
                    last_progress_time = current_time
                
                # Check if there are more results
                if result['is_truncated']:
                    next_key_marker = result['next_key_marker']
                    next_version_marker = result['next_version_marker']
                else:
                    print("✓ Processing complete!")
                    # Clean up resume state on successful completion
                    if enable_resume and self.resume_state_file:
                        self._cleanup_resume_state()
                    break
        except KeyboardInterrupt:
            print("\n")
            # Save checkpoint on cancellation
            if enable_resume:
                self._save_resume_state(bucket_name, prefix, next_key_marker, 
                                      next_version_marker, batch_num - 1, total_items, output_file)
                print(f"Progress saved to checkpoint. Run again to resume.")
            raise
        finally:
            # Signal CSV writer to stop
            self.write_queue.put(None)
            if self.writer_thread:
                self.writer_thread.join()
            
            # Save final state if there was an error
            if enable_resume and self.error_count > 0:
                self._save_resume_state(bucket_name, prefix, next_key_marker, 
                                      next_version_marker, self.current_batch_num, total_items, output_file)
        
        # Calculate and format execution time (actual runtime excluding time between sessions)
        end_time = time.time()
        session_runtime = end_time - self.session_start_time
        total_runtime = getattr(self, 'cumulative_runtime', 0) + session_runtime
        total_seconds = int(total_runtime)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        if hours > 0:
            time_str = f"{hours} hours, {minutes} minutes"
        elif minutes > 0:
            time_str = f"{minutes} minutes, {seconds} seconds"
        else:
            time_str = f"{seconds} seconds"
        
        print(f"\n=== Export Summary ===")
        print(f"Completed export for bucket: {bucket_name}")
        if prefix:
            print(f"Using prefix: {prefix}")
        print(f"Records written to CSV: {self.total_processed:,}")
        print(f"Total object versions: {total_items:,}")
        print(f"Errors encountered: {self.error_count:,}")
        print(f"Output file: {output_file}")
        if enable_resume and self.error_count > 0:
            print(f"Resume state file: {self.resume_state_file}")
            print("To resume: run the same command again")
        print(f"Completed in {time_str}")
        return  # Explicit return after successful completion

def main():
    """Main function for single bucket processing."""
    parser = argparse.ArgumentParser(
        description='Export S3 object versions to CSV using boto3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python s3_versions_boto3.py --bucket my-bucket --output versions.csv
  
  # With prefix
  python s3_versions_boto3.py --bucket my-bucket --prefix logs/2024/ --output versions.csv
  
  # With specific AWS profile
  python s3_versions_boto3.py --bucket my-bucket --profile my-profile --output versions.csv
  
  # With specific region
  python s3_versions_boto3.py --bucket my-bucket --region us-west-2 --output versions.csv
  
  # Skip CSV headers
  python s3_versions_boto3.py --bucket my-bucket --nocsvheaders --output versions.csv
  
  # Skip versioning check (for users without s3:GetBucketVersioning permission)
  python s3_versions_boto3.py --bucket my-bucket --skipversioningcheck --output versions.csv
  
  # Disable resume capability (always start fresh)
  python s3_versions_boto3.py --bucket my-bucket --noresume --output versions.csv
  
  # BOPS manifest compatible output (no headers, 3 columns only)
  python s3_versions_boto3.py --bucket my-bucket --bopsmanifestcompatible --output versions.csv
  
  # Disable URL encoding of key names
  python s3_versions_boto3.py --bucket my-bucket --nourlencoding --output versions.csv
  
  # Resume from previous interrupted run (automatic)
  python s3_versions_boto3.py --bucket my-bucket --output versions.csv

Requirements:
  pip install boto3

AWS Credentials:
  Configure using one of these methods:
  - AWS CLI: aws configure
  - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  - IAM roles (if running on EC2)
  - AWS profiles: --profile my-profile
        """
    )
    
    parser.add_argument(
        '--bucket',
        required=True,
        help='S3 bucket name to process'
    )
    
    parser.add_argument(
        '--prefix',
        help='Object key prefix to filter results'
    )
    
    parser.add_argument(
        '--output',
        default='s3_object_versions.csv',
        help='Output CSV file path (default: s3_object_versions.csv)'
    )
    
    parser.add_argument(
        '--profile',
        help='AWS profile name to use (optional)'
    )
    
    parser.add_argument(
        '--region',
        help='AWS region name (optional, uses default region if not specified)'
    )
    
    parser.add_argument(
        '--nocsvheaders',
        action='store_true',
        help='Skip writing CSV headers (first row)'
    )
    
    parser.add_argument(
        '--skipversioningcheck',
        action='store_true',
        help='Skip bucket versioning check (removes need for s3:GetBucketVersioning permission)'
    )
    
    parser.add_argument(
        '--noresume',
        action='store_true',
        help='Disable resume capability (always start fresh)'
    )
    
    parser.add_argument(
        '--bopsmanifestcompatible',
        action='store_true',
        help='BOPS manifest compatible mode: removes CSV headers and outputs only 3 columns'
    )
    
    parser.add_argument(
        '--nourlencoding',
        action='store_true',
        help='Disable URL encoding of key names (enabled by default)'
    )
    

    
    args = parser.parse_args()
    
    try:
        exporter = S3VersionsExporter(
            profile_name=args.profile,
            region_name=args.region
        )
        

        # BOPS manifest compatible mode overrides CSV headers
        write_headers = not args.nocsvheaders and not args.bopsmanifestcompatible
        
        exporter.export_versions(
            bucket_name=args.bucket,
            prefix=args.prefix,
            output_file=args.output,
            skip_versioning_check=args.skipversioningcheck,
            write_headers=write_headers,
            enable_resume=not args.noresume,
            bops_compatible=args.bopsmanifestcompatible,
            url_encode=not args.nourlencoding
        )
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()