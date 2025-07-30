import requests
import json
import time
import logging
from typing import Dict, Any, List, Optional
from .config import PDScanConfig

import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from email.headerregistry import Address
import os

def send_webhook(event: str, payload: Dict[str, Any], config: PDScanConfig = None, logger=None):
    """
    Gửi webhook notification theo config.
    :param event: Tên sự kiện (scan_complete, scan_failed, report_generated...)
    :param payload: Dữ liệu JSON gửi đi
    :param config: Đối tượng PDScanConfig hoặc None (tự tạo)
    :param logger: Logger để log lại kết quả
    :return: True nếu gửi thành công, False nếu thất bại
    """
    if config is None:
        config = PDScanConfig()
    webhook_cfg = config.get_webhook_config()
    if not webhook_cfg.get('enabled', False):
        return False
    if 'events' in webhook_cfg and event not in webhook_cfg['events']:
        return False
    url = webhook_cfg.get('url')
    timeout = webhook_cfg.get('timeout', 5)
    max_retries = webhook_cfg.get('max_retries', 3)
    headers = {'Content-Type': 'application/json'}
    payload = dict(payload)
    payload['event'] = event
    payload['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, data=json.dumps(payload), headers=headers, timeout=timeout)
            if resp.status_code >= 200 and resp.status_code < 300:
                if logger:
                    logger.info(f"Webhook sent: {event} -> {url} (status {resp.status_code})")
                return True
            else:
                if logger:
                    logger.warning(f"Webhook failed: {event} -> {url} (status {resp.status_code})")
        except Exception as e:
            if logger:
                logger.error(f"Webhook error: {event} -> {url} (attempt {attempt}): {e}")
        time.sleep(1)
    return False

def notify_scan_complete(user_id, scan_id, matches_count, status, config=None, logger=None):
    payload = {
        'user_id': user_id,
        'scan_id': scan_id,
        'status': status,
        'matches_count': matches_count
    }
    return send_webhook('scan_complete', payload, config, logger)

def notify_scan_failed(user_id, scan_id, error_message, config=None, logger=None):
    payload = {
        'user_id': user_id,
        'scan_id': scan_id,
        'status': 'failed',
        'error': error_message
    }
    return send_webhook('scan_failed', payload, config, logger)

def notify_report_generated(user_id, scan_id, report_format, report_url=None, config=None, logger=None):
    payload = {
        'user_id': user_id,
        'scan_id': scan_id,
        'status': 'report_generated',
        'format': report_format,
        'report_url': report_url
    }
    return send_webhook('report_generated', payload, config, logger)

# --- EMAIL NOTIFICATION ---
def send_email(subject: str, body: str, to: List[str], config: PDScanConfig = None, logger=None, attachments: Optional[List[str]] = None) -> bool:
    """
    Gửi email notification theo config.
    :param subject: Tiêu đề email
    :param body: Nội dung email (plain text)
    :param to: Danh sách email nhận
    :param config: Đối tượng PDScanConfig hoặc None (tự tạo)
    :param logger: Logger để log lại kết quả
    :param attachments: Danh sách file đính kèm (nếu có)
    :return: True nếu gửi thành công, False nếu thất bại
    """
    import ssl
    if config is None:
        config = PDScanConfig()
    email_cfg = config.get_email_config()
    if not email_cfg.get('enabled', False):
        return False
    smtp_server = email_cfg.get('smtp_server')
    smtp_port = email_cfg.get('smtp_port', 587)
    smtp_user = email_cfg.get('smtp_user')
    smtp_password = email_cfg.get('smtp_password')
    sender = email_cfg.get('sender')
    recipients = to or email_cfg.get('recipients', [])
    use_tls = email_cfg.get('use_tls', True)
    max_retries = email_cfg.get('max_retries', 3)
    timeout = email_cfg.get('timeout', 10)
    
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg.set_content(body)
    
    # Đính kèm file nếu có
    if attachments:
        for file_path in attachments:
            if not os.path.isfile(file_path):
                continue
            with open(file_path, 'rb') as f:
                file_data = f.read()
                file_name = os.path.basename(file_path)
                msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)
    
    for attempt in range(1, max_retries + 1):
        try:
            if use_tls:
                context = ssl.create_default_context()
                with smtplib.SMTP(smtp_server, smtp_port, timeout=timeout) as server:
                    server.starttls(context=context)
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
            else:
                with smtplib.SMTP(smtp_server, smtp_port, timeout=timeout) as server:
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
            if logger:
                logger.info(f"Email sent: {subject} -> {recipients}")
            return True
        except Exception as e:
            if logger:
                logger.error(f"Email error: {subject} -> {recipients} (attempt {attempt}): {e}")
            time.sleep(1)
    return False

def notify_scan_complete_email(user_id, scan_id, matches_count, status, config=None, logger=None):
    email_cfg = config.get_email_config() if config else PDScanConfig().get_email_config()
    if not email_cfg.get('enabled', False):
        return False
    if 'events' in email_cfg and 'scan_complete' not in email_cfg['events']:
        return False
    subject = f"[PDScan] Scan Complete: {scan_id}"
    body = f"User: {user_id}\nScan ID: {scan_id}\nStatus: {status}\nMatches: {matches_count}\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    recipients = email_cfg.get('recipients', [])
    return send_email(subject, body, recipients, config, logger)

def notify_scan_failed_email(user_id, scan_id, error_message, config=None, logger=None):
    email_cfg = config.get_email_config() if config else PDScanConfig().get_email_config()
    if not email_cfg.get('enabled', False):
        return False
    if 'events' in email_cfg and 'scan_failed' not in email_cfg['events']:
        return False
    subject = f"[PDScan] Scan Failed: {scan_id}"
    body = f"User: {user_id}\nScan ID: {scan_id}\nStatus: FAILED\nError: {error_message}\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    recipients = email_cfg.get('recipients', [])
    return send_email(subject, body, recipients, config, logger)

def notify_report_generated_email(user_id, scan_id, report_format, report_file=None, config=None, logger=None):
    email_cfg = config.get_email_config() if config else PDScanConfig().get_email_config()
    if not email_cfg.get('enabled', False):
        return False
    if 'events' in email_cfg and 'report_generated' not in email_cfg['events']:
        return False
    subject = f"[PDScan] Report Generated: {scan_id}"
    body = f"User: {user_id}\nScan ID: {scan_id}\nStatus: REPORT GENERATED\nFormat: {report_format}\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    recipients = email_cfg.get('recipients', [])
    attachments = [report_file] if report_file else None
    return send_email(subject, body, recipients, config, logger, attachments)

def send_slack(event: str, message: str, config: PDScanConfig = None, logger=None) -> bool:
    """
    Gửi Slack notification theo config.
    :param event: Tên sự kiện (scan_complete, scan_failed, report_generated...)
    :param message: Nội dung gửi lên Slack
    :param config: Đối tượng PDScanConfig hoặc None (tự tạo)
    :param logger: Logger để log lại kết quả
    :return: True nếu gửi thành công, False nếu thất bại
    """
    import requests
    import time
    if config is None:
        config = PDScanConfig()
    slack_cfg = config.get_slack_config()
    if not slack_cfg.get('enabled', False):
        return False
    if 'events' in slack_cfg and event not in slack_cfg['events']:
        return False
    url = slack_cfg.get('webhook_url')
    timeout = slack_cfg.get('timeout', 5)
    max_retries = slack_cfg.get('max_retries', 3)
    payload = {
        'text': message
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, json=payload, timeout=timeout)
            if resp.status_code >= 200 and resp.status_code < 300:
                if logger:
                    logger.info(f"Slack sent: {event} -> {url} (status {resp.status_code})")
                return True
            else:
                if logger:
                    logger.warning(f"Slack failed: {event} -> {url} (status {resp.status_code})")
        except Exception as e:
            if logger:
                logger.error(f"Slack error: {event} -> {url} (attempt {attempt}): {e}")
        time.sleep(1)
    return False

def notify_scan_complete_slack(user_id, scan_id, matches_count, status, config=None, logger=None):
    slack_cfg = config.get_slack_config() if config else PDScanConfig().get_slack_config()
    if not slack_cfg.get('enabled', False):
        return False
    if 'events' in slack_cfg and 'scan_complete' not in slack_cfg['events']:
        return False
    message = f":white_check_mark: *PDScan Complete*\nUser: `{user_id}`\nScan ID: `{scan_id}`\nStatus: `{status}`\nMatches: `{matches_count}`\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    return send_slack('scan_complete', message, config, logger)

def notify_scan_failed_slack(user_id, scan_id, error_message, config=None, logger=None):
    slack_cfg = config.get_slack_config() if config else PDScanConfig().get_slack_config()
    if not slack_cfg.get('enabled', False):
        return False
    if 'events' in slack_cfg and 'scan_failed' not in slack_cfg['events']:
        return False
    message = f":x: *PDScan Failed*\nUser: `{user_id}`\nScan ID: `{scan_id}`\nStatus: `FAILED`\nError: `{error_message}`\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    return send_slack('scan_failed', message, config, logger)

def notify_report_generated_slack(user_id, scan_id, report_format, report_url=None, config=None, logger=None):
    slack_cfg = config.get_slack_config() if config else PDScanConfig().get_slack_config()
    if not slack_cfg.get('enabled', False):
        return False
    if 'events' in slack_cfg and 'report_generated' not in slack_cfg['events']:
        return False
    message = f":page_facing_up: *PDScan Report Generated*\nUser: `{user_id}`\nScan ID: `{scan_id}`\nStatus: `REPORT GENERATED`\nFormat: `{report_format}`\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    if report_url:
        message += f"\nReport: {report_url}"
    return send_slack('report_generated', message, config, logger) 