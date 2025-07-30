"""
Report generation for PDScan
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from jinja2 import Template

class ReportGenerator:
    """Generate reports in various formats"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_html_report(self, scan_results: List[Dict], scan_info: Dict, metrics: Dict = None) -> str:
        """Generate HTML report"""
        html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>PDScan Report - {{ scan_info.timestamp }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background: #f0f0f0; padding: 20px; border-radius: 5px; }
        .summary { background: #e8f4f8; padding: 15px; margin: 20px 0; border-radius: 5px; }
        .matches { margin: 20px 0; }
        .match-item { 
            background: #fff; 
            border: 1px solid #ddd; 
            padding: 10px; 
            margin: 5px 0; 
            border-radius: 3px; 
        }
        .high-risk { border-left: 5px solid #ff4444; }
        .medium-risk { border-left: 5px solid #ffaa00; }
        .low-risk { border-left: 5px solid #44aa44; }
        .metrics { background: #f9f9f9; padding: 15px; margin: 20px 0; border-radius: 5px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <div class="header">
        <h1>PDScan Security Report</h1>
        <p>Generated on: {{ scan_info.timestamp }}</p>
        <p>Target: {{ scan_info.url }}</p>
    </div>
    
    <div class="summary">
        <h2>Scan Summary</h2>
        <table>
            <tr><td>Total Matches:</td><td>{{ scan_info.total_matches }}</td></tr>
            <tr><td>Scan Duration:</td><td>{{ "%.2f"|format(scan_info.duration) }} seconds</td></tr>
            <tr><td>Sample Size:</td><td>{{ scan_info.sample_size }}</td></tr>
            <tr><td>User:</td><td>{{ scan_info.user_id }}</td></tr>
        </table>
    </div>
    
    {% if metrics %}
    <div class="metrics">
        <h2>Performance Metrics</h2>
        <table>
            <tr><td>Total Scans:</td><td>{{ metrics.total_scans }}</td></tr>
            <tr><td>Success Rate:</td><td>{{ "%.1f"|format(metrics.success_rate * 100) }}%</td></tr>
            <tr><td>Average Duration:</td><td>{{ "%.2f"|format(metrics.avg_duration) }}s</td></tr>
            <tr><td>Error Rate:</td><td>{{ "%.1f"|format(metrics.error_rate * 100) }}%</td></tr>
        </table>
    </div>
    {% endif %}
    
    <div class="matches">
        <h2>Detected Matches ({{ scan_results|length }})</h2>
        {% for match in scan_results %}
        <div class="match-item {% if match.risk_level == 'high' %}high-risk{% elif match.risk_level == 'medium' %}medium-risk{% else %}low-risk{% endif %}">
            <h3>{{ match.path }}</h3>
            <p><strong>Value:</strong> {{ match.value[:100] }}{% if match.value|length > 100 %}...{% endif %}</p>
            <p><strong>Pattern:</strong> {{ match.pattern }}</p>
            <p><strong>Risk Level:</strong> {{ match.risk_level|upper }}</p>
            {% if match.context %}
            <p><strong>Context:</strong> {{ match.context }}</p>
            {% endif %}
        </div>
        {% endfor %}
    </div>
    
    <div style="margin-top: 40px; padding: 20px; background: #f9f9f9; border-radius: 5px;">
        <h3>Recommendations</h3>
        <ul>
            <li>Review all high-risk matches immediately</li>
            <li>Implement data encryption for sensitive fields</li>
            <li>Consider data masking for non-production environments</li>
            <li>Regular security audits are recommended</li>
        </ul>
    </div>
</body>
</html>
        """
        
        template = Template(html_template)
        html_content = template.render(
            scan_results=scan_results,
            scan_info=scan_info,
            metrics=metrics
        )
        
        filename = f"pdscan_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return str(filepath)
    
    def generate_json_report(self, scan_results: List[Dict], scan_info: Dict, metrics: Dict = None) -> str:
        """Generate JSON report"""
        report_data = {
            'report_info': {
                'generated_at': datetime.now().isoformat(),
                'version': '1.0',
                'format': 'json'
            },
            'scan_info': scan_info,
            'scan_results': scan_results,
            'metrics': metrics,
            'summary': {
                'total_matches': len(scan_results),
                'high_risk_count': len([m for m in scan_results if m.get('risk_level') == 'high']),
                'medium_risk_count': len([m for m in scan_results if m.get('risk_level') == 'medium']),
                'low_risk_count': len([m for m in scan_results if m.get('risk_level') == 'low'])
            }
        }
        
        filename = f"pdscan_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        
        return str(filepath)
    
    def generate_csv_report(self, scan_results: List[Dict], scan_info: Dict) -> str:
        """Generate CSV report"""
        import csv
        
        filename = f"pdscan_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Path', 'Value', 'Pattern', 'Risk Level', 'Context'])
            
            for match in scan_results:
                writer.writerow([
                    match.get('path', ''),
                    match.get('value', '')[:100],  # Truncate long values
                    match.get('pattern', ''),
                    match.get('risk_level', 'unknown'),
                    match.get('context', '')
                ])
        
        return str(filepath)
    
    def generate_pdf_report(self, scan_results: List[Dict], scan_info: Dict, metrics: Dict = None) -> str:
        """Generate PDF report (requires reportlab)"""
        try:
            from reportlab.lib.pagesizes import letter
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.lib import colors
            
            filename = f"pdscan_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            filepath = self.output_dir / filename
            
            doc = SimpleDocTemplate(str(filepath), pagesize=letter)
            styles = getSampleStyleSheet()
            story = []
            
            # Title
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=16,
                spaceAfter=30
            )
            story.append(Paragraph("PDScan Security Report", title_style))
            story.append(Spacer(1, 12))
            
            # Scan Info
            story.append(Paragraph("Scan Information", styles['Heading2']))
            scan_info_data = [
                ['Generated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['Target URL', scan_info.get('url', 'N/A')],
                ['Total Matches', str(scan_info.get('total_matches', 0))],
                ['Duration', f"{scan_info.get('duration', 0):.2f} seconds"],
                ['User', scan_info.get('user_id', 'N/A')]
            ]
            
            scan_table = Table(scan_info_data, colWidths=[2*inch, 4*inch])
            scan_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(scan_table)
            story.append(Spacer(1, 20))
            
            # Matches
            if scan_results:
                story.append(Paragraph("Detected Matches", styles['Heading2']))
                matches_data = [['Path', 'Value', 'Pattern', 'Risk Level']]
                
                for match in scan_results[:50]:  # Limit to first 50 matches
                    matches_data.append([
                        match.get('path', '')[:30],
                        match.get('value', '')[:30],
                        match.get('pattern', ''),
                        match.get('risk_level', 'unknown').upper()
                    ])
                
                matches_table = Table(matches_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1*inch])
                matches_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                story.append(matches_table)
            
            doc.build(story)
            return str(filepath)
            
        except ImportError:
            raise ImportError("PDF generation requires reportlab. Install with: pip install reportlab")
    
    def generate_all_reports(self, scan_results: List[Dict], scan_info: Dict, metrics: Dict = None) -> Dict[str, str]:
        """Generate all report formats"""
        reports = {}
        
        # Generate HTML report
        reports['html'] = self.generate_html_report(scan_results, scan_info, metrics)
        
        # Generate JSON report
        reports['json'] = self.generate_json_report(scan_results, scan_info, metrics)
        
        # Generate CSV report
        reports['csv'] = self.generate_csv_report(scan_results, scan_info)
        
        # Generate PDF report (if reportlab is available)
        try:
            reports['pdf'] = self.generate_pdf_report(scan_results, scan_info, metrics)
        except ImportError:
            reports['pdf'] = "PDF generation skipped (reportlab not installed)"
        
        return reports 