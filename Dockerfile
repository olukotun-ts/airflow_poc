FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# RUN sudo apt-get install gcc python3-dev

# Install dependencies
RUN pip install -r requirements.txt

# Set environment variables
ENV NYT_API_KEY=${NYT_API_KEY} \
    NYT_API_SECRET=${NYT_API_SECRET} \
    OPENAI_API_KEY=${OPENAI_API_KEY}

# Expose port 5000
EXPOSE 5000

# Run the process_articles.py script
CMD ["python", "process_articles.py"]
