#create s3 bucket to store template
aws s3 mb s3://sam-bucket-test-kj

#create cloudformation package
aws cloudformation package --s3-bucket sam-bucket-test-kj2 --template-file template.yaml --output-template-file gen/template-generated.yaml

#deploy
aws cloudformation deploy --template-file gen/template-generated.yaml --stack-name test-only-kj2 --capabilities CAPABILITY_NAMED_IAM

