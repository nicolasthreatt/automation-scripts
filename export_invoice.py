import json
import os
import requests

from argparse import ArgumentParser
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import Any, Dict, List, Optional


load_dotenv()


class InvoiceExporter:
    """
    Export BILL invoices and download invoice PDFs.

    Uses the BILL v3 invoices list endpoint with createdTime filters and the
    invoice PDF download endpoint to fetch PDF bytes. For list API details, see:
    https://developer.bill.com/reference/listinvoices
    """
    BASE_URL = "https://gateway.prod.bill.com/connect"
    PDF_BASE_URL = "https://api.bill.com"

    def __init__(
        self,
        username: str,
        password: str,
        org_id: str,
        dev_key: str,
    ):
        """
        Args:
            username (str): BILL username for login.
            password (str): BILL password for login.
            org_id (str): BILL organization ID (starts with 008).
            dev_key (str): BILL developer key.
        """
        self.base_url = self.BASE_URL
        self.pdf_base_url = self.PDF_BASE_URL
        self.dev_key = dev_key
        self.session_id = self._login(username, password, org_id, dev_key)

    def _login(self, username: str, password: str, org_id: str, dev_key: str) -> str:
        url = f"{self.base_url}/v3/login"
        login = {
            "username": username,
            "password": password,
            "organizationId": org_id,
            "devKey": dev_key,
        }
        resp = requests.post(url, json=login, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        session_id = data.get("sessionId")
        if not session_id:
            raise RuntimeError(f"Login succeeded but sessionId missing. Response: {data}")
        return session_id

    def list_invoices_created_between(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/v3/invoices"
        headers = {
            "content-type": "application/json",
            "sessionId": self.session_id,
            "devKey": self.dev_key,
        }
        filters = f'createdTime:gte:"{start_date}",createdTime:lt:"{end_date}"'

        results: List[Dict[str, Any]] = []
        next_page: Optional[str] = None

        while True:
            params = {"page": next_page} if next_page else {"filters": filters}

            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            results.extend(data.get("results", []))
            next_page = data.get("nextPage")
            if not next_page:
                break

        return results

    def get_invoice_pdf(self, invoice_id: str) -> bytes:
        url = f"{self.pdf_base_url}/Invoice2PdfServlet"
        headers = {"sessionId": self.session_id}
        params = {"Id": invoice_id, "PresentationType": "PDF"}
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        return resp.content

    def export_pdfs(self, invoices: List[Dict[str, Any]], out_dir: Path):
        seen: Dict[str, int] = {}

        for invoice in invoices:
            invoice_id = invoice.get("id")
            if not invoice_id:
                continue

            invoice_date = invoice.get("invoiceDate")
            if not invoice_date:
                print(f"Skipping invoice {invoice_id} (missing invoiceDate).")
                continue

            pdf = self.get_invoice_pdf(invoice_id)

            raw = str(invoice_date).strip()
            raw = raw.split("T", 1)[0].split(" ", 1)[0]
            base_name = raw.replace("/", "-").replace(":", "-")
            count = seen.get(base_name, 0)
            seen[base_name] = count + 1
            filename = f"{base_name}.pdf" if count == 0 else f"{base_name}-{invoice_id}.pdf"

            pdf_path = out_dir / filename
            pdf_path.write_bytes(pdf)


def main():
    parser = ArgumentParser(description="Export Bill.com invoices by createdTime range.")
    parser.add_argument("--start-date", required=True, help="Start date (MM-DD-YYYY), inclusive.")
    parser.add_argument("--end-date", required=True, help="End date (MM-DD-YYYY), exclusive.")
    parser.add_argument("--out-dir", default="invoices", help="Directory for exported JSON.")
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start_date, "%m-%d-%Y")
    end_dt = datetime.strptime(args.end_date, "%m-%d-%Y")
    if start_dt >= end_dt:
        raise ValueError("start-date must be earlier than end-date.")
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")

    username = os.getenv("BILL_USERNAME", "")
    password = os.getenv("BILL_PASSWORD", "")
    org_id = os.getenv("BILL_ORG_ID", "")
    dev_key = os.getenv("BILL_DEV_KEY", "")

    required = {
        "BILL_USERNAME": username,
        "BILL_PASSWORD": password,
        "BILL_ORG_ID": org_id,
        "BILL_DEV_KEY": dev_key,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars in .env: {', '.join(missing)}")

    exporter = InvoiceExporter(
        username=username,
        password=password,
        org_id=org_id,
        dev_key=dev_key,
    )
    invoices = exporter.list_invoices_created_between(start_date, end_date)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"bill_invoices_{start_date}_to_{end_date}.json"
    out_path.write_text(json.dumps(invoices, indent=2), encoding="utf-8")

    exporter.export_pdfs(invoices, out_dir)

    print(f"Fetched {len(invoices)} invoices in from {start_date} to {end_date}.")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
