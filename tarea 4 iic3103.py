import pandas as pd
import requests
import xml.etree.ElementTree as et
import gspread
from gspread_dataframe import set_with_dataframe

def script():
    """
        PARA CORRER CREAR ARCHIVO EN EL DIRECTORIO CON ESTE NOMBRE : t4-iic3103-c4f85436ed98.json
        TAMBIEN ESCRIBIR EL SIGUIENTE CODIGO EN EL ARCHIVO:

        {
            "type": "service_account",
            "project_id": "t4-iic3103",
            "private_key_id": "c4f85436ed983289a11cf25e8ad88847044aaa8d",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDT5h9aQnJCh6Ch\nn3W/xYckKMt/wGdP/scvEPmNVElJydxFKRtV2muMk68i0TZD+NpTGw7SJtVdH2sv\nBjA/zRvygMLC/xUMJceHODJc+QyGcMfpmxvjYjDJdVVveyHZNBh9B91eWcE997q6\nS7Z3uSRrpNiPlX8ROJrXSz+norAQ6hq0YTSPLXTZ/8QN+OiJGsbV8g1IS9WvXhPE\n5toalTE+RvG6F91KD31PoukCoYIhDAHYJ+ogQuWcgGC9liMsFLGppg/4h0lvWLmS\nBgGsL2mAz5Gz35DEg8Rohm8KYBmoxOM/lHkHWvzxFOViHSyUDXD5dkcTClxX51zR\n+m4S+dHNAgMBAAECggEAAbu/1TOyPgQ0TloGUfgfKlinZXE7/thzsu9gFmfs8Qla\nSrxwmWZ+x5Phzdev9YPjI1NU/D+3F8fupRuvmgeG+1pfn7NxMi2hmBbHfEbjugEX\n0WxjTxwtM1Hut8CvGcdTFjM73WnwQa+ySV9RM5+SrUsPXWPg81YJP4pTGtV/nV89\nnXXcMt8dt6+b42JLEL5sCMxbHHHPLsCfKbc8CKU98vNXGv6yypAK/ZSlT5kxv0fu\nmJW23tc103n/4eLlD7CwjgkD9g6rTBCB/cVyu+8PntLKmjbu38WZITQe6J9MGowS\nhwfXiTzqxOkekdCHMFX87bE8GLHu27w/E0YHmcSUAQKBgQD8f21rv4w45G+hyErZ\nbgrcYIM2c1n0STJuedEogTXqst1Cw6iaXcZ1OXxlnFYYp3P6TwXRRbxEKUZHIHVk\nDJKGcSIGBqZPevyd45KtKqk1PKr+SxNWGxW6vll8xDTuD0PEUL4Uni4rXHjJhNUt\nIl4+SrmIzy4JdPIWabo1L5slzQKBgQDW1ok9XZ0K0hNKcKsMoWbBjhkKmv1lMRhs\n0IUm4auGfRj3JDzasUSpzRu8Da0wwvmlC31n06h5mH2EZ5Ny11NSTnGS87KlF8KR\n0JSSfqqa4z7z30claua+Nad4JDEOVwA27Zt1Jblii+pcPI5PU+EBMtsoac8LD1mC\n4qQ0r+1cAQKBgGlSmgjs4IfGuc3pJ1ek8EDWvUTUPvyIH9I3XAiiRSQ+Mc8soEKr\nxSGWtg4IdV1ZVCZgGhQeG5bvBJKctnriR6huvnt3AJ58ta0ChWdYOTastMLX/BvZ\ntmmsBtXlXhLDKUhVScR96tJJg1TD5Mwr3tXrA5NOoGxvbJ/uwC55SiUtAoGAdim9\nhkEM9TyiW5kom2PSrrbHUyv4H0YnlwIBmmcY/nmj15rY1bgWegZUMNSc42r6ACsX\nqJmj2m1i7AZD9TA1pdl7YkYjheZRn/3HGeQNVe+TWnL+ILhxfUDpgYz2Zlq3xX5e\nuMos8uTd3Cl6ZGtQIGamX84CkKS6qOCbTFdPCAECgYBNPmZxA9WKLRd+v5yao90X\nVd/2lX1jU6P4VQCsBTErZ6IikZOkM+4u/ksHIKXZ1pxWLMjwAHViMHuUwIY6goF1\nW+782tlP57SzUb+XfQ9re3urTRHjpWmR1k/uAVB0wM9a+6hkBqgzuJTxyLaoVn4Q\nF3qp2kMJCaTubdcwjRQ/lQ==\n-----END PRIVATE KEY-----\n",
            "client_email": "sjkinast@t4-iic3103.iam.gserviceaccount.com",
            "client_id": "108887774044386019117",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/sjkinast%40t4-iic3103.iam.gserviceaccount.com"
        }
    """
    

    def url(country):
        return f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{country}.xml'

    death_indicators = ['Number of deaths', 'Number of infant deaths','Number of under-five deaths',
    'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)', 
    'Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)', 
    'Estimates of number of homicides', 'Crude suicide rates (per 100 000 population)', 
    'Mortality rate attributed to unintentional poisoning (per 100 000 population)', 
    'Number of deaths attributed to non-communicable diseases, by type of disease and sex', 
    'Estimated road traffic death rate (per 100 000 population)', 'Estimated number of road traffic deaths']

    weight_indicators = ['Mean BMI (kg/m&#xb2;) (crude estimate)', 'Mean BMI (kg/m&#xb2;) (age-standardized estimate)', 
    'Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)', 
    'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)', 
    'Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)', 
    'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)', 
    'Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)', 
    'Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)']

    other_indicators = ['Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)', 
    'Estimate of daily cigarette smoking prevalence (%)', 'Estimate of daily tobacco smoking prevalence (%)', 
    'Estimate of current cigarette smoking prevalence (%)', 'Estimate of current cigarette smoking prevalence (%)', 
    'Estimate of current tobacco smoking prevalence (%)', 'Mean systolic blood pressure (crude estimate)', 
    'Mean fasting blood glucose (mmol/l) (crude estimate)', 'Mean Total Cholesterol (crude estimate)']

    indicators = death_indicators + weight_indicators + other_indicators

    countries = ['CHL','CUB', 'BEL', 'CRI', 'DNK', 'ESP']

    df = pd.DataFrame(columns=['COUNTRY','SEX','YEAR','GHECAUSES','AGEGROUP','Display','Numeric','Low', 'High', 'GHO'])

    index = 0

    for country in countries:
        r = requests.get(url(country))
        if r.status_code == 200 and index < 5000000:
            string_xml = r.content
            tree = et.fromstring(string_xml)
            for child in tree:
                for column in df.columns:
                    element = child.findtext(column)
                    df.at[index, column] = element if element else None
                index += 1
    key1 = df['GHO'].isin(indicators)
    df = df[key1]
    df['Numeric'] = df['Numeric'].astype(float)
    df['Low'] = df['Low'].astype(float)
    df['High'] = df['High'].astype(float)
    # ACCES GOOGLE SHEET
    gc = gspread.service_account(filename='t4-iic3103-c4f85436ed98.json') #AQUI VA EL NOMBRE DEL ARCHIVO .JSON CON LA CONFIGURACION
    sh = gc.open_by_key('1A9A5-CoKSHi7zaCBaQSAk_yO7cAzmf6suHMwLzKrEeM')
    worksheet = sh.get_worksheet(0) #-> 0 - first sheet, 1 - second sheet etc.
    worksheet.clear()
    set_with_dataframe(worksheet, df) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET
    return 0

if __name__ == '__main__':
    print(f'Exit Code {script()}')