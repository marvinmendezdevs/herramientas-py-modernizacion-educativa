import pandas as pd
from pathlib import Path

data = Path("data")
output = Path("output")

def main():
    consum = pd.read_csv(next(data.glob("consumo.csv")), low_memory=False)
    consum = consum[consum["Type"] == "Clase"]
    consum["Subject"] = consum["Subject"].str.replace("Lenguaje y Literatura", "Lenguaje")

    print(consum.head())
    consum.to_csv(output / "consume_by_class.csv")
    
main()