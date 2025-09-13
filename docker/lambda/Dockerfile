# docker/lambda/Dockerfile
FROM public.ecr.aws/lambda/python:3.11

# Copy requirements and install dependencies
COPY requirements-lambda.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements-lambda.txt

# Copy function code
COPY src/ingestion/ ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["lambda_function.lambda_handler"]