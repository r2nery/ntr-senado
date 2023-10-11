from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from alive_progress import alive_bar
import lxml
import concurrent.futures

def fetch_discurso_data(row):
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
                discurso_html.encoding = 'utf-8'
                discurso_soup = BeautifulSoup(discurso_html.text, "html.parser")
                discurso = " ".join([p.text for p in discurso_soup.find_all("p")])
                discurso = re.sub(r"\n", " ", discurso)
                cols_data.append(discurso.strip())
        else:
            cols_data.append(ele.text.strip())
    return cols_data

def url2(href):
    return "https://www.camara.leg.br/internet/sitaqweb/" + href

dates = {"10/10/2000": "01/01/2001"}
dates.update({f"01/01/20{str(i).zfill(2)}": f"01/01/20{str(i+1).zfill(2)}" for i in range(1, 24)})
dates = tuple(dates.items())

page_size = 50

try:
    df = pd.read_csv("data.csv")
    starting_date = df["Data"].iloc[0]
except:
    df = pd.DataFrame(columns=["Data", "Sessão", "Fase", "URL_Discurso", "Discurso", "Orador", "Hora", "Publicação"])

for data_inicio, data_fim in dates:
    gen_url = lambda page: f"https://www.camara.leg.br/internet/sitaqweb/resultadoPesquisaDiscursos.asp?CurrentPage={page}&BasePesq=plenario&dtInicio={data_inicio}&dtFim={data_fim}&CampoOrdenacao=dtSessao&TipoOrdenacao=ASC&PageSize={page_size}"
    url = gen_url(1)
    response = requests.get(url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, "lxml")
    vstr = soup.find_all("span", {"class": "visualStrong"})
    num_citations = int(vstr[-1].text.replace(".", ""))
    num_pages = num_citations // page_size + 2

    with alive_bar(num_citations) as bar:
        for page in range(1, num_pages):
            url = gen_url(page)
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "lxml")
            table = soup.find("table", {"class": "table table-bordered variasColunas"})
            rows = table.find_all("tr", {"class": "even"})
            rows += table.find_all("tr", {"class": "odd"})
            rows = [row for row in rows if not row.get("id")]

            # Use ThreadPoolExecutor to speed up fetching discursos
            with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
                results = list(executor.map(fetch_discurso_data, rows))

            for cols_data in results:
                temp_df = pd.DataFrame([cols_data], columns=["Data", "Sessão", "Fase", "URL_Discurso", "Discurso", "Orador", "Hora", "Publicação"])
                df = pd.concat([df, temp_df])
                bar()

            # Save the dataframe to the file after processing each page
            df.to_csv("data.csv", index=False)

