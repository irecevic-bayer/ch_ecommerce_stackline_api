# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import json
import logging
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import requests
from requests.models import Response
from isoweek import Week

from models.ExportResponse import ExportResponse

list_of_companies = [{"company_name":"Amazon Germany","company_id":"14"},{"company_name":"ACME","company_id":"184"},{"company_name":"ASOS","company_id":"193"},{"company_name":"Albertsons","company_id":"94"},{"company_name":"Albertsons Companies","company_id":"170"},{"company_name":"Amazon Australia","company_id":"31"},{"company_name":"Amazon Belgium","company_id":"119"},{"company_name":"Amazon Brazil","company_id":"33"},{"company_name":"Amazon Canada","company_id":"11"},{"company_name":"Amazon China","company_id":"34"},{"company_name":"Amazon Egypt","company_id":"103"},{"company_name":"Amazon France","company_id":"15"},{"company_name":"Amazon Fresh","company_id":"43"},{"company_name":"Amazon India","company_id":"28"},{"company_name":"Amazon Italy","company_id":"29"},{"company_name":"Amazon Japan","company_id":"16"},{"company_name":"Amazon KSA","company_id":"72"},{"company_name":"Amazon Mexico","company_id":"32"},{"company_name":"Amazon Netherlands","company_id":"62"},{"company_name":"Amazon Poland","company_id":"90"},{"company_name":"Amazon Spain","company_id":"30"},{"company_name":"Amazon Sweden","company_id":"77"},{"company_name":"Amazon Turkey","company_id":"74"},{"company_name":"Amazon UAE","company_id":"53"},{"company_name":"Amazon UK","company_id":"17"},{"company_name":"Amazon US","company_id":"1"},{"company_name":"Asda","company_id":"20"},{"company_name":"BJ's","company_id":"173"},{"company_name":"Babies R Us Canada","company_id":"161"},{"company_name":"Best Buy Canada","company_id":"106"},{"company_name":"Best Buy US","company_id":"3"},{"company_name":"Bloomingdales","company_id":"152"},{"company_name":"CVS","company_id":"95"},{"company_name":"Chewy","company_id":"18"},{"company_name":"Costco Business Delivery US","company_id":"153"},{"company_name":"Costco Canada","company_id":"107"},{"company_name":"Costco US","company_id":"9"},{"company_name":"Crate & Barrel","company_id":"190"},{"company_name":"Dermstore","company_id":"194"},{"company_name":"Dillard's","company_id":"195"},{"company_name":"Dollar General","company_id":"120"},{"company_name":"First Cry UAE","company_id":"177"},{"company_name":"Flipkart","company_id":"35"},{"company_name":"Food 4 Less","company_id":"133"},{"company_name":"Food Lion","company_id":"126"},{"company_name":"Fred Meyer","company_id":"127"},{"company_name":"Fresh Direct","company_id":"154"},{"company_name":"Freshop","company_id":"155"},{"company_name":"Fry’s Food","company_id":"145"},{"company_name":"GameStop","company_id":"121"},{"company_name":"Giant Company","company_id":"146"},{"company_name":"Giant Eagle","company_id":"149"},{"company_name":"Giant Eagle","company_id":"149"},{"company_name":"Giant Food","company_id":"114"},{"company_name":"Giant Martin's","company_id":"115"},{"company_name":"Gopuff","company_id":"140"},{"company_name":"H-E-B","company_id":"117"},{"company_name":"Hannaford","company_id":"142"},{"company_name":"Home Depot","company_id":"6"},{"company_name":"Hy-Vee","company_id":"148"},{"company_name":"Instacart ACME Markets","company_id":"63_44"},{"company_name":"Instacart ALDI","company_id":"63_12"},{"company_name":"Instacart Albertsons","company_id":"63_233"},{"company_name":"Instacart BJ's Wholesale Club","company_id":"63_26"},{"company_name":"Instacart Canada","company_id":"100"},{"company_name":"Instacart Costco","company_id":"63_5"},{"company_name":"Instacart Food Lion","company_id":"63_133"},{"company_name":"Instacart Fry's","company_id":"63_201"},{"company_name":"Instacart H-E-B","company_id":"63_45"},{"company_name":"Instacart Jewel-Osco","company_id":"63_10"},{"company_name":"Instacart King Soopers","company_id":"63_56"},{"company_name":"Instacart Kroger","company_id":"63_58"},{"company_name":"Instacart Pick 'n Save","company_id":"63_128"},{"company_name":"Instacart Publix","company_id":"63_57"},{"company_name":"Instacart QFC","company_id":"63_42"},{"company_name":"Instacart Ralphs","company_id":"63_38"},{"company_name":"Instacart Safeway","company_id":"63_1"},{"company_name":"Instacart Sam's Club","company_id":"63_352"},{"company_name":"Instacart Shaw’s","company_id":"63_375"},{"company_name":"Instacart Smith's","company_id":"63_252"},{"company_name":"Instacart Sprouts Farmers Market","company_id":"63_279"},{"company_name":"Instacart Stop & Shop","company_id":"63_235"},{"company_name":"Instacart Target","company_id":"63_648"},{"company_name":"Instacart Total Wine & More","company_id":"63_144"},{"company_name":"Instacart US","company_id":"63"},{"company_name":"Instacart Vons","company_id":"63_290"},{"company_name":"Instacart Walmart","company_id":"63_1487"},{"company_name":"Instacart Wegmans","company_id":"63_231"},{"company_name":"JD.com","company_id":"26"},{"company_name":"Jewel Osco","company_id":"183"},{"company_name":"King Soopers","company_id":"132"},{"company_name":"Kroger","company_id":"48"},{"company_name":"L'Equipeur Canada","company_id":"162"},{"company_name":"Lazada Malaysia","company_id":"89"},{"company_name":"Lazada Philippines","company_id":"57"},{"company_name":"Lazada Singapore","company_id":"56"},{"company_name":"Lazada Thailand","company_id":"99"},{"company_name":"Lowe's","company_id":"73"},{"company_name":"Lowe's Canada","company_id":"163"},{"company_name":"Lowes US","company_id":"73"},{"company_name":"Macy's","company_id":"139"},{"company_name":"Mark's Canada","company_id":"164"},{"company_name":"Meijer","company_id":"91"},{"company_name":"Mercado Libre Brazil","company_id":"59"},{"company_name":"Mercado Libre Mexico","company_id":"75"},{"company_name":"Metro Canada","company_id":"165"},{"company_name":"Michael's","company_id":"156"},{"company_name":"Mumz World","company_id":"178"},{"company_name":"NewEgg.com","company_id":"37"},{"company_name":"Noon","company_id":"118"},{"company_name":"Nordstrom","company_id":"52"},{"company_name":"Nordstrom Rack","company_id":"157"},{"company_name":"Ocado","company_id":"22"},{"company_name":"Office Depot","company_id":"38"},{"company_name":"Pavilions","company_id":"187"},{"company_name":"Pet Supplies Plus","company_id":"172"},{"company_name":"PetSmart","company_id":"116"},{"company_name":"Petco","company_id":"98"},{"company_name":"Pick'n Save","company_id":"134"},{"company_name":"Publix","company_id":"150"},{"company_name":"QFC","company_id":"93"},{"company_name":"RONA Canada","company_id":"166"},{"company_name":"Ralphs","company_id":"128"},{"company_name":"Randalls","company_id":"186"},{"company_name":"Rite Aid","company_id":"196"},{"company_name":"Safeway","company_id":"92"},{"company_name":"Saks Fifth Ave","company_id":"197"},{"company_name":"Sam's Club Mexico","company_id":"176"},{"company_name":"Sam's Club US","company_id":"25"},{"company_name":"Sephora","company_id":"50"},{"company_name":"Shaw's","company_id":"171"},{"company_name":"Shipt","company_id":"158"},{"company_name":"ShopRite","company_id":"147"},{"company_name":"Shopee Indonesia","company_id":"81"},{"company_name":"Shopee Malaysia","company_id":"78"},{"company_name":"Shopee Philippines","company_id":"80"},{"company_name":"Shopee Singapore","company_id":"82"},{"company_name":"Shopee Thailand","company_id":"83"},{"company_name":"Shopee Vietnam","company_id":"79"},{"company_name":"Shoppers Drug Mart","company_id":"84"},{"company_name":"Skinstore","company_id":"198"},{"company_name":"Smith's Food and Drug","company_id":"131"},{"company_name":"Sportchek Canada","company_id":"167"},{"company_name":"Staples","company_id":"5"},{"company_name":"Stop & Shop","company_id":"113"},{"company_name":"Target","company_id":"4"},{"company_name":"Tesco","company_id":"24"},{"company_name":"Tom Thumb","company_id":"185"},{"company_name":"Toys R Us Canada","company_id":"168"},{"company_name":"Uber Eats Canada","company_id":"169"},{"company_name":"Uber Eats US","company_id":"159"},{"company_name":"Ulta","company_id":"160"},{"company_name":"Vons","company_id":"188"},{"company_name":"Walgreens","company_id":"96"},{"company_name":"Walmart Canada","company_id":"12"},{"company_name":"Walmart OPD","company_id":"49"},{"company_name":"Walmart US","company_id":"2"},{"company_name":"Wayfair","company_id":"7"},{"company_name":"Wegmans","company_id":"174"},{"company_name":"Whole Foods","company_id":"58"},{"company_name":"Williams Sonoma","company_id":"191"}]

def get_credentials():
    # Replace `path to the .json file` with the actual file path
    credentials_file_path = r"./.secret/credentials.json"

    with open(credentials_file_path, "r") as f:
        credentials = json.load(f)
    return credentials

def get_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {
        'Content-Type': 'application/json',
        'Connection': 'keep-alive',
        'Authorization': auth_header
    }
    return headers


def fire_queue_request(export_data_type: str, week_id: str) -> str:
    queue_url: str = f'https://{hostname}/api/dataexport/QueueDataExportByWeek'
    params: str = f'weekId={week_id}&retailerId=14&dataType={export_data_type}'
    url = f'{queue_url}?{params}'
    headers = get_headers()
    response: Response = requests.request("GET", url, headers=headers, data={}, timeout=request_timeout_seconds)
    print_my(f'Queue: {response.text}')
    return response.text


def fire_status_request(request_id: str) -> str:
    queue_url: str = f'https://{hostname}/api/dataexport/GetDataExportRequestStatus'
    params: str = f'requestId={request_id}'
    url = f'{queue_url}?{params}'
    headers = get_headers()
    response: Response = requests.request("GET", url, headers=headers, data={}, timeout=request_timeout_seconds)
    print_my(f'\tStatus: {response.text}')
    return response.text


def fire_download_request(request_id: str) -> Response:
    queue_url: str = f'https://{hostname}/api/dataexport/DownloadDataExportResult'
    params: str = f'requestId={request_id}'
    url = f'{queue_url}?{params}'
    headers = get_headers()
    response: Response = requests.request("GET", url, headers=headers, data={}, timeout=request_timeout_seconds, stream=True)
    # print_my(response.text)
    return response


def wait_for_export_to_finish(request_id: str):
    retry_max: int = 2
    while True:
        try:
            response: str = fire_status_request(request_id)
            status_response: ExportResponse = ExportResponse.from_json(response)
            if status_response.success and status_response.status == 'finished successfully':
                print_my(f'File ready for download. RequestId: {request_id}')
                return
        except Exception as e:
            print_my(f'Exception in wait_for_export_to_finish(). RequestId: {request_id}')
            logging.error(traceback.format_exc())
        time.sleep(4)
    # print_my(f'All {retry_max} retries failed in wait_for_export_to_finish(). RequestId: {request_id}')


def download_file(request_id: str, file_name: str):
    retry_max: int = 2
    for retry in range(0, retry_max):
        try:
            response: Response = fire_download_request(request_id)
            with open(file_name, 'wb') as f:
                for chunk in response.raw.stream(1024, decode_content=False):
                    if chunk:
                        f.write(chunk)
            print_my(f'{file_name} downloaded.')
            return
        except Exception as e:
            print_my(f'Exception in download_file(). RequestId: {request_id}')
            logging.error(traceback.format_exc())
        time.sleep(2)
    print_my(f'All {retry_max} retries failed in download_file(). RequestId: {request_id}')


def queue_data_type_for_week(export_data_type, week_id) -> ExportResponse:
    retry_max: int = 2
    for retry in range(0, retry_max):
        try:
            print_my(f'Queuing Export for: {export_data_type} | {week_id}')
            response: str = fire_queue_request(export_data_type, week_id)
            if response is None:
                raise Exception("Response None")
            export_response: ExportResponse = ExportResponse.from_json(response)
            if export_response.success:
                return export_response
        except Exception as e:
            print_my(f'Exception in queue_data_type_for_week().')
            logging.error(traceback.format_exc())
        time.sleep(1)
    print_my(f'All {retry_max} retries failed in queue_data_type_for_week() for week {week_id} and data type {export_data_type}.')


def export_data_type_for_week(export_data_type, week_id, year):
    try:
        export_response = queue_data_type_for_week(export_data_type, week_id)
        request_id = export_response.requestId
        print_my(f'Checking export status for: {export_data_type} | {week_id} | RequestId: {request_id}')
        wait_for_export_to_finish(request_id)
        print_my(f'Downloading file for: {export_data_type} | {week_id} | RequestId: {request_id}')
        file_name: str = f'{export_dir}/{year}/{export_data_type}/retailer__{export_data_type}__{week_id}.tsv.gz'
        Path(file_name).parent.mkdir(parents=True, exist_ok=True)
        download_file(request_id, file_name)
    except Exception as e:
        print_my(f'Exception in export_data_type_for_week() for: {export_data_type} | {week_id}')
        logging.error(traceback.format_exc())


def run_exports():
    print_my('Starting the job ...')
    print_my(f'\tRequest Timeout in seconds: {request_timeout_seconds}')
    years = [2024]
    export_data_types: List[str] = ['beacon-sales', 'beacon-buybox', 'beacon-advertising', 'beacon-content-score', 'beacon-ratings-reviews']
    # export_data_types: List[str] = ['atlas-sales', 'atlas-promotions', 'atlas-traffic-keywords','atlas-traffic-products', 'atlas-content-score', 'atlas-ratings-reviews']

    current_week = Week.thisweek()
    for export_data_type in export_data_types:
        for year in years:
            print_my('\n')
            print_my('-' * 100)
            print_my(f'Starting export of {export_data_type} for year {year} ...')
            last_week = get_max_week(year, current_week)
            print_my(f'Last week identified...{last_week}...')
            for week in range(1, last_week + 1):
                week_id: str = get_week_id(year, week)
                export_data_type_for_week(export_data_type, week_id, year)


def get_max_week(year, current_week):
    if year == current_week.year:
        return current_week.week - 1
    last_week = Week.last_week_of_year(year).week
    return last_week


def get_week_id(year: int, week: int) -> str:
    week_id: str = f'{year}{week:0>2d}'
    return week_id


def print_my(param):
    print(f'{datetime.now()}: {param}')

credentials = get_credentials()

server: str = 'api'
hostname: str = f'{server}.stackline.com'
auth_header: str = credentials["api_key"]
request_timeout_seconds = 5 * 60
current_week: int = 47
export_dir: str = './stackline-export/'

if __name__ == '__main__':
    # TO DO
    # get following arguments
    # 1. retailer ID to collect
    # 2. specific period to download
    # 3. specific type of report name: atlas or beacon
    # 4. specific report to download: ...
    # 5. save to specific folder in local/ upload to cloud and remove from local
    run_exports()
