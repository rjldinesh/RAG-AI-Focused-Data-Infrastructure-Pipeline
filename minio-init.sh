#!/bin/sh

echo "⏳ Waiting for MinIO to be ready for Spark setup..."

until mc alias set localminio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD; do
  echo "Waiting for MinIO..."
  sleep 3
done

echo "✅ Connected to MinIO. Creating bucket and folders..."

mc mb localminio/activefence-bucket || echo "Bucket already exists."

touch /tmp/emptyfile

for folder in raw bronze silver gold; do
  mc cp /tmp/emptyfile localminio/activefence-bucket/bbc_tech/$folder/_placeholder || echo "$folder exists"
done

rm /tmp/emptyfile

cat <<EOF > /tmp/spark-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::activefence-bucket"
    },
    {
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::activefence-bucket/*"
    }
  ]
}
EOF

mc admin policy create localminio spark-policy /tmp/spark-policy.json || echo "Policy exists"

mc admin user add localminio $MINIO_ACCESS_KEY $MINIO_SECRET_KEY || echo "User exists"

mc admin policy attach localminio spark-policy --user $MINIO_ACCESS_KEY

echo "✅ Spark access configured."
