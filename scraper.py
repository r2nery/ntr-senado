from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import json
from alive_progress import alive_bar
import lxml
import os

page_size = 1000
dates = tuple((f'01/01/{year}', f'31/12/{year}') for year in range(2000, 2024))
dates = (('10/10/2000', dates[0][1]),) + dates[1:]
gen_url = (
    lambda page, page_size, data_inicio, data_fim: f"https://www.camara.leg.br/internet/sitaqweb/resultadoPesquisaDiscursos.asp?CurrentPage={page}&BasePesq=plenario&dtInicio={data_inicio}&dtFim={data_fim}&CampoOrdenacao=dtSessao&TipoOrdenacao=ASC&PageSize={page_size}"
)
url2 = lambda href: "https://www.camara.leg.br/internet/sitaqweb/" + href

# Check for the latest year CSV in directory and continue scraping from the next year
existing_files = [f for f in os.listdir() if f.endswith('.csv') and f[:-4].isdigit()]
existing_files.sort(reverse=True)

if existing_files:
    last_year_scraped = int(existing_files[0].split('.')[0])
    dates = [date for date in dates if int(date[0][-4:]) > last_year_scraped]

df_columns = ["Data", "Sessão", "Fase", "URL_Discurso", "Discurso", "Orador", "Hora", "Publicação"]

for data_inicio, data_fim in dates:
    year_data = []
    url = gen_url(1, page_size, data_inicio, data_fim)
    response = requests.get(url)
    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "lxml")
    vstr = soup.find_all("span", {"class": "visualStrong"})
    num_citations = int(vstr[-1].text.replace(".", ""))
    num_pages = num_citations // page_size + 2
    with alive_bar(total=num_citations, title=f"Scraping year {data_inicio[-4:]}") as bar:
        for page in range(1, num_pages):
            try:
                url = gen_url(page, page_size, data_inicio, data_fim)
                response = requests.get(url)
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "lxml")
                table = soup.find("table", {"class": "table table-bordered variasColunas"})
                rows = table.find_all("tr", {"class": "even"})
                rows += table.find_all("tr", {"class": "odd"})
                rows = [row for row in rows if not row.get("id")]
            except Exception as e:
                # this exception handles the case where the page has no table
                print(f"Exception: {e}")
                continue
            for row in rows:
                cols_data = []
                eles = row.find_all("td")
                for ele in eles:
                    if ele.get("align"):
                        href = str(ele.a.get("href"))
                        href = re.sub(r"\s+", "", href)
                        if href.startswith("TextoHTML"):
                            discurso_url = url2(href)
                            cols_data.append(discurso_url)
                            discurso_html = requests.get(discurso_url)
                            discurso_html.encoding = "utf-8"
                            discurso_soup = BeautifulSoup(discurso_html.text, "html.parser")
                            discurso = " ".join([p.text for p in discurso_soup.find_all("p")])
                            discurso = re.sub(r"\n", " ", discurso)
                            cols_data.append(discurso.strip())
                    else:
                        cols_data.append(ele.text.strip())
                year_data.append(cols_data)
                bar()
    
    # Save the year data to a CSV after scraping the entire year
    df_year = pd.DataFrame(year_data, columns=df_columns)
    df_year.to_csv(f"{data_inicio[-4:]}.csv", index=False)
