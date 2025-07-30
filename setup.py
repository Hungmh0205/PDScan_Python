from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="pdscan",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Scan your data stores for unencrypted personal data (PII)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/pdscan",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
    install_requires=[
        "click>=8.0.0",
        "boto3>=1.26.0",
        "elasticsearch>=8.0.0",
        "pymongo>=4.0.0",
        "redis>=4.0.0",
        "psycopg2-binary>=2.9.0",
        "mysql-connector-python>=8.0.0",
        "oracledb>=1.0.0",
        "sqlalchemy>=2.0.0",
        "pandas>=2.0.0",
        "celery>=5.0.0",
        "pyyaml>=6.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.22.0",
        "python-magic>=0.4.27",
        "openpyxl>=3.1.0",
        "opensearch-py>=2.3.0"
    ],
    entry_points={
        "console_scripts": [
            "pdscan=pdscan.cmd.root:main",
        ],
    },
) 