# 🔍 PDScan - Personal Data Scanner

**PDScan** là một công cụ mạnh mẽ để quét và phát hiện dữ liệu cá nhân (PII - Personally Identifiable Information) chưa được mã hóa trong các kho dữ liệu của bạn.

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE.txt)
[![Status](https://img.shields.io/badge/Status-Beta-orange.svg)](https://github.com/yourusername/pdscan)

## 📋 Mục lục

- [Tính năng](#-tính-năng)
- [Cài đặt](#-cài-đặt)
- [Sử dụng nhanh](#-sử-dụng-nhanh)
- [Hướng dẫn chi tiết](#-hướng-dẫn-chi-tiết)
- [Cấu hình](#-cấu-hình)
- [API](#-api)
- [Ví dụ](#-ví-dụ)
- [Hỗ trợ](#-hỗ-trợ)

## ✨ Tính năng

### 🔐 Bảo mật & Tuân thủ
- **Phát hiện PII**: Tự động phát hiện email, số thẻ tín dụng, SSN, số điện thoại
- **Mã hóa dữ liệu**: Hỗ trợ mã hóa kết quả quét
- **RBAC**: Kiểm soát quyền truy cập dựa trên vai trò
- **Audit Log**: Ghi log đầy đủ các hoạt động quét

### 🗄️ Hỗ trợ nhiều nguồn dữ liệu
- **SQL Databases**: PostgreSQL, MySQL, SQLite, MariaDB, Oracle
- **NoSQL**: MongoDB, Redis
- **Search Engines**: Elasticsearch, OpenSearch
- **Cloud Storage**: AWS S3
- **Local Files**: Files và thư mục cục bộ

### 🚀 Hiệu suất cao
- **Quét song song**: Hỗ trợ multi-processing
- **Connection Pooling**: Tối ưu kết nối database
- **Async Support**: Hỗ trợ quét bất đồng bộ cho Oracle
- **Distributed Scanning**: Quét phân tán với Celery

### 📊 Báo cáo & Thông báo
- **Multiple Formats**: JSON, CSV, SQLite, Text
- **Email Notifications**: Thông báo qua email
- **Slack Integration**: Tích hợp với Slack
- **Webhook Support**: Webhook cho các sự kiện

## 🛠️ Cài đặt

### Yêu cầu hệ thống
- Python 3.7+
- pip hoặc conda

### Cài đặt từ source

```bash
# Clone repository
git clone https://github.com/yourusername/pdscan.git
cd pdscan

# Tạo virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# hoặc
venv\Scripts\activate     # Windows

# Cài đặt dependencies
pip install -r requirements.txt

# Cài đặt package
pip install -e .
```

### Cài đặt từ PyPI (khi có)

```bash
pip install pdscan
```

## 🚀 Sử dụng nhanh

### Quét cơ bản

```bash
# Quét MongoDB
pdscan mongodb://localhost:27017/mydb

# Quét PostgreSQL
pdscan postgresql://user:pass@localhost:5432/mydb

# Quét file local
pdscan file:///path/to/files
```

### Quét với tùy chọn

```bash
# Quét với hiển thị dữ liệu
pdscan postgresql://localhost/mydb --show-data

# Quét chỉ định pattern
pdscan mongodb://localhost/mydb --only-patterns email,credit-card

# Quét với output JSON
pdscan redis://localhost --format json --output results.json
```

## 📖 Hướng dẫn chi tiết

### 1. Cú pháp lệnh

```bash
pdscan [OPTIONS] URL
```

### 2. Tham số URL

| Scheme | Ví dụ | Mô tả |
|--------|--------|-------|
| `mongodb://` | `mongodb://localhost:27017/db` | MongoDB database |
| `postgresql://` | `postgresql://user:pass@host:5432/db` | PostgreSQL database |
| `mysql://` | `mysql://user:pass@host:3306/db` | MySQL database |
| `oracle://` | `oracle://user:pass@host:1521/service` | Oracle database |
| `redis://` | `redis://localhost:6379` | Redis database |
| `elasticsearch://` | `elasticsearch://localhost:9200` | Elasticsearch |
| `s3://` | `s3://bucket-name/path` | AWS S3 bucket |
| `file://` | `file:///path/to/files` | Local files |

### 3. Tùy chọn quét

#### Tùy chọn cơ bản
```bash
--config PATH           # File cấu hình YAML
--show-data            # Hiển thị dữ liệu tìm thấy
--show-all             # Hiển thị tất cả fields
--sample-size N        # Số lượng documents mẫu (default: 1000)
--processes N          # Số processes song song (default: 1)
```

#### Tùy chọn lọc
```bash
--only COLLECTIONS     # Chỉ quét collections/tables này
--except COLLECTIONS   # Bỏ qua collections/tables này
--min-count N          # Số matches tối thiểu để báo cáo
--pattern REGEX        # Pattern tùy chỉnh
--only-patterns LIST   # Chỉ quét patterns này (email,credit-card,ssn)
```

#### Tùy chọn output
```bash
--format FORMAT        # Định dạng output (text,json,csv,sqlite)
--output FILE          # File output
--debug               # Bật debug mode
```

### 4. Ví dụ sử dụng

#### Quét MongoDB với tùy chọn
```bash
pdscan mongodb://localhost:27017/mydb \
  --show-data \
  --only-patterns email,credit-card \
  --format json \
  --output results.json
```

#### Quét PostgreSQL với lọc
```bash
pdscan postgresql://user:pass@localhost:5432/prod \
  --only users,orders \
  --except logs,temp \
  --min-count 5 \
  --processes 4
```

#### Quét Oracle async
```bash
pdscan oracle://user:pass@host:1521/service \
  --processes 10 \
  --sample-size 5000 \
  --show-data
```

## ⚙️ Cấu hình

### File cấu hình YAML

Tạo file `config/pdscan.yaml`:

```yaml
database:
  connections:
    - name: "prod-postgres"
      type: "postgresql"
      url: "postgresql://user:pass@host:5432/db"
      ssl: true
      pool_size: 10
      timeout: 30
      retry_attempts: 3
    
    - name: "oracle-prod"
      type: "oracle"
      url: "oracle://user:pass@host:1521/service"
      host: "host"
      port: 1521
      service_name: "service"
      user: "user"
      password: "pass"
      ssl: false
      pool_size: 5
      timeout: 30
      retry_attempts: 3
      fetch_size: 1000
      target_schema: ""

security:
  ssl_verify: true
  certificate_path: "/path/to/certs"
  encryption_key: "${ENCRYPTION_KEY}"

scanning:
  batch_size: 1000
  max_workers: 4
  timeout: 300

logging:
  level: "INFO"
  file: "./pdscan.log"
  rotation: "daily"

webhook:
  enabled: true
  url: "http://localhost:9000/webhook"
  timeout: 5
  max_retries: 3
  events:
    - scan_complete
    - scan_failed
    - report_generated

email:
  enabled: true
  smtp_server: "smtp.example.com"
  smtp_port: 587
  smtp_user: "noreply@example.com"
  smtp_password: "yourpassword"
  sender: "noreply@example.com"
  recipients:
    - "recipient@example.com"
  use_tls: true
  events:
    - scan_complete
    - scan_failed
    - report_generated

slack:
  enabled: true
  webhook_url: "https://hooks.slack.com/services/xxx/yyy/zzz"
  timeout: 5
  max_retries: 3
  events:
    - scan_complete
    - scan_failed
    - report_generated

oracle:
  pool_max: 20
```

### Biến môi trường

```bash
# Authentication
export PDSCAN_USER_ID="your-user-id"
export PDSCAN_API_KEY="your-api-key"

# Encryption
export ENCRYPTION_KEY="your-encryption-key"

# Database credentials
export DB_PASSWORD="your-db-password"
```

## 🌐 API

### Chạy API Server

```bash
# Chạy với cấu hình mặc định
python run_api.py

# Chạy với tùy chọn
python run_api.py --host 0.0.0.0 --port 8000 --debug
```

### API Endpoints

#### Health Check
```bash
curl http://localhost:8000/api/v1/health
```

#### Scan Database
```bash
curl -X POST http://localhost:8000/api/v1/scan \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "postgresql://localhost/mydb",
    "options": {
      "show_data": true,
      "only_patterns": ["email", "credit-card"]
    }
  }'
```

#### Get Scan Results
```bash
curl http://localhost:8000/api/v1/results/SCAN_ID \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### API Documentation

- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc

## 📝 Ví dụ

### Ví dụ Python

```python
import asyncio
from pdscan.internal.oracle_adapter_async import OracleAdapterAsync
from pdscan.internal.scan_opts import ScanOptions

async def scan_oracle():
    # Configuration
    config = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'ORCL',
        'max_concurrent_tables': 5,
        'pool_min': 3,
        'pool_max': 10
    }
    
    # Create adapter
    adapter = OracleAdapterAsync('oracle://user:pass@host:port/service', config)
    
    # Scan options
    options = ScanOptions(
        show_data=True,
        show_all=False,
        only_patterns=['credit_card', 'email']
    )
    
    # Run scan
    matches = await adapter.scan(options)
    
    # Print results
    print(f"Found {len(matches)} matches:")
    for match in matches:
        print(f"- {match['rule']} in {match['table']}.{match['column']}")

# Run
asyncio.run(scan_oracle())
```

### Ví dụ với Makefile

```bash
# Install
make install

# Run tests
make test

# Lint code
make lint

# Clean build files
make clean

# Build package
make build

# Build Docker image
make docker
```

## 🔧 Troubleshooting

### Lỗi thường gặp

#### 1. Connection Error
```bash
# Kiểm tra kết nối database
pdscan postgresql://localhost/test --debug
```

#### 2. Permission Denied
```bash
# Kiểm tra quyền user
export PDSCAN_USER_ID="admin"
pdscan mongodb://localhost/mydb
```

#### 3. Memory Issues
```bash
# Giảm batch size
pdscan postgresql://localhost/mydb --sample-size 100
```

### Debug Mode

```bash
# Bật debug để xem chi tiết lỗi
pdscan postgresql://localhost/mydb --debug
```

### Log Files

- **Audit Log**: `logs/audit.log`
- **Metrics**: `logs/metrics.json`
- **Application Log**: `pdscan.log`

## 🤝 Đóng góp

1. Fork repository
2. Tạo feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Tạo Pull Request

## 📄 License

Dự án này được cấp phép theo MIT License - xem file [LICENSE.txt](LICENSE.txt) để biết chi tiết.

## 🆘 Hỗ trợ

### Tài liệu
- [API Documentation](http://localhost:8000/api/docs)
- [Examples](examples/)
- [Configuration Guide](config/)

### Liên hệ
- **Email**: support@pdscan.com
- **Issues**: [GitHub Issues](https://github.com/yourusername/pdscan/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/pdscan/discussions)

### Community
- **Slack**: [Join our Slack](https://pdscan.slack.com)
- **Discord**: [Join our Discord](https://discord.gg/pdscan)

---

**Made with ❤️ by the PDScan Team** 