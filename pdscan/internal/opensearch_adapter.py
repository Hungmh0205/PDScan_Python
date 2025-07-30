from opensearchpy import OpenSearch, RequestsHttpConnection
from .elasticsearch_adapter import ElasticsearchAdapter

class OpenSearchAdapter(ElasticsearchAdapter):
    """Adapter cho OpenSearch, kế thừa từ ElasticsearchAdapter nhưng dùng opensearch-py."""
    def connect(self) -> None:
        from urllib.parse import urlparse
        parsed = urlparse(self.url)
        if parsed.scheme != "opensearch":
            raise ValueError("Invalid OpenSearch URL scheme")
        self.client = OpenSearch(
            [self.url],
            http_compress=True,
            use_ssl=self._ssl,
            verify_certs=self._ssl,
            connection_class=RequestsHttpConnection,
        )
        self.client.ping()

    def __init__(self, url: str, config: dict = None):
        super().__init__(url, config) 