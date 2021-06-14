import pandas as pd
import requests
import xml.etree.ElementTree as et
import gspread
from gspread_dataframe import set_with_dataframe

def script():

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
        if r.status_code == 200:
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
    gc = gspread.service_account(filename='/home/santiago/Escritorio/t4-iic3103-c4f85436ed98.json')
    sh = gc.open_by_key('1A9A5-CoKSHi7zaCBaQSAk_yO7cAzmf6suHMwLzKrEeM')
    worksheet = sh.get_worksheet(0) #-> 0 - first sheet, 1 - second sheet etc.
    worksheet.clear()
    set_with_dataframe(worksheet, df) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET
    return 0

if __name__ == '__main__':
    print(f'Exit Code {script()}')