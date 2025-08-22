# Use the official Dataflow base image for Python 3 Flex Templates.
# This image contains the necessary launcher logic to start the pipeline.
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Set the required environment variables for the Flex Template launcher.
# These tell the launcher where to find the main Python file and dependencies.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements.txt

# Copy the requirements file to the template directory.
COPY requirements.txt /template/

# Install the Python dependencies. This is done in a separate step to leverage Docker layer caching.
RUN pip install --no-cache-dir -r /template/requirements.txt

# Copy the rest of the pipeline code to the template directory.
COPY . /template/