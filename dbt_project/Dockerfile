FROM python:3.10-slim

# Cài đặt dbt cho BigQuery
RUN pip install --no-cache-dir dbt-bigquery==1.11.0

# Tạo thư mục làm việc
WORKDIR /usr/app/dbt

# Copy toàn bộ code dbt của bạn vào container
COPY . .

# Mặc định khi container chạy sẽ gọi lệnh dbt
ENTRYPOINT ["dbt"]