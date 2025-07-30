from celery import Celery
import importlib

# Celery app, dùng Redis làm broker mặc định
celery_app = Celery(
    'pdscan',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

@celery_app.task
def distributed_scan(adapter_name, scan_opts, rules):
    """
    Task Celery để thực hiện quét phân tán.
    adapter_name: tên adapter (vd: 'mongodb_adapter')
    scan_opts: dict các tuỳ chọn quét
    rules: dict các rule quét
    """
    # Nạp adapter động
    adapter_module = importlib.import_module(f'pdscan.internal.{adapter_name}')
    AdapterClass = getattr(adapter_module, 'Adapter')
    adapter = AdapterClass(**scan_opts)
    # Giả sử match_finder có hàm scan
    from pdscan.internal.match_finder import scan
    result = scan(adapter, rules)
    return result 