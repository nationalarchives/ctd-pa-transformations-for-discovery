import pytest

# This test file performs live S3 communication and should be skipped during normal pytest runs.
pytest.skip("Skipping live S3 bucket communication tests (network / credentials required)", allow_module_level=True)

# Original implementation retained below for manual execution (python tests/test_buckets.py) if needed.
import boto3  # pragma: no cover
import sys    # pragma: no cover
from pathlib import Path  # pragma: no cover
repo_root = Path(__file__).resolve().parents[1]  # pragma: no cover
sys.path.insert(0, str(repo_root))  # pragma: no cover
from src.config_loader import UniversalConfig  # pragma: no cover
from botocore.exceptions import ClientError  # pragma: no cover

config = UniversalConfig(yaml_file="config.yaml", base_path=repo_root)
# Debug: Check what config loaded
print(f"Debug - Config file path: {repo_root / 'config.yaml'}")
print(f"Debug - Config file exists: {(repo_root / 'config.yaml').exists()}")
print(f"Debug - YAML config contents: {config.yaml_config}")
print(f"Debug - All config keys: {list(config.yaml_config.keys())}")
print(f"Debug - bucket_name: {config.get('bucket_name')}")

def setup_s3_client():
    """Setup S3 client using boto3 with specified profile"""
    try:
        bucket_name = config.get("bucket_name")
        print(f"Using bucket: {bucket_name}")
        session = boto3.Session(profile_name='ax-prod')
        s3 = session.client('s3')
        if bucket_name is None:
            raise ValueError("Bucket name not found in configuration.")
    except ValueError:
            print("Bucket name not found in configuration.")
            sys.exit(1)
    except Exception as e:
        print(f"Error setting up S3 client: {e}")
        sys.exit(1)
    return s3, bucket_name

def test_bucket_communication(bucket_name):  # pragma: no cover
    """Test various operations with the S3 bucket"""
    
    try:
        # Test 1: Check if bucket exists and is accessible
        print(f"Testing bucket: {bucket_name}")
        
        # Test 2: Get bucket location
        location = s3.get_bucket_location(Bucket=bucket_name)
        print(f"✅ Bucket location: {location.get('LocationConstraint', 'eu-west-2')}")
        
        # Test 3: List objects in bucket (first 10)
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=10)
        if 'Contents' in response:
            print(f"✅ Found {len(response['Contents'])} objects (showing first 10):")
            for obj in response['Contents'][:5]:  # Show first 5
                print(f"   - {obj['Key']} (Size: {obj['Size']} bytes)")
        else:
            print("✅ Bucket is empty or no objects found")
        
        # Test 4: Get bucket versioning status
        versioning = s3.get_bucket_versioning(Bucket=bucket_name)
        print(f"✅ Versioning status: {versioning.get('Status', 'Not enabled')}")
        
        # Test 5: Check bucket policy (if accessible)
        try:
            policy = s3.get_bucket_policy(Bucket=bucket_name)
            print("✅ Bucket policy exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
                print("ℹ️  No bucket policy configured")
            else:
                print(f"⚠️  Cannot read bucket policy: {e.response['Error']['Code']}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"❌ Bucket '{bucket_name}' does not exist")
        elif error_code == 'AccessDenied':
            print(f"❌ Access denied to bucket '{bucket_name}'")
        elif error_code == 'Forbidden':
            print(f"❌ Forbidden access to bucket '{bucket_name}'")
        else:
            print(f"❌ Error accessing bucket: {error_code} - {e.response['Error']['Message']}")
        return False
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

# Run the test
if __name__ == "__main__":
    print(f"Using config file at: {repo_root / 'config.yaml'}")
    print("🔍 Testing S3 bucket communication...")
    s3, bucket_name = setup_s3_client()
    success = test_bucket_communication(bucket_name)
    
    if success:
        print("\n🎉 Bucket communication test completed successfully!")
    else:
        print("\n💥 Bucket communication test failed!")

