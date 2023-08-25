# Step 1: Define the base image
FROM python:3.11

# Step 2: Copy application code and dependencies
WORKDIR /jbm_backend
COPY requirements.txt /jbm_backend
RUN pip install --no-cache-dir -r requirements.txt

# Step 3: Copy the rest of your application code
COPY . /jbm_backend

# Step 4: Set the working directory
WORKDIR /jbm_backend

# Step 5: Expose the port your Flask app is listening on (replace 5000 with your actual port number)
EXPOSE 5000
# Step 6: Install Gunicorn
RUN pip install gunicorn

# Step 7: Specify the command to run your Flask app with Gunicorn
CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:5000", "jbm_backend:app"]

