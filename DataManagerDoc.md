# Binance DataManager

### Data manager to handle binance market data

<br>

### Want to contact me ? 👋

https://discord.gg/wfpGXvjj9t

 ##### Dependencies :

 - Pandas
 - Numpy
 - ccxt
 
 ## Doc 📝

---

#### Initialisation
```python
manager = DataManager()
```

##### Optional parameters:
- path: the path where is located your data file or where you want it to be saved. Make it None if you want to disable
        file feature
- market: the market you want to load the data. Example: "BTC/USDT"
- timeframe: usually "1m", "5m", "15m", "30m", "1h", "1d", "1w", "1M", "1y"
- since: the timestamp of the first candle to download
- limit: the number of candles you want to download
- multithreading: False to disable multithreading download True to enable it, it will hugely increase download speed
- download_size: Amount of requests scheduled at the same time if you are encountering issues disable it or
                 lower the amount of requests scheduled at the same time using
                 size parameter, if you want to increase the download speed increase this parameter but this is not 
                 recommended and can cause issues.
---

#### Load your data
```python
manager.load()
```
---

####  Access your data
```python
manager.main_dataframe
```
##### Return:
A pandas dataframe containing your data.
Columns: [timestamp | open | high | low | close | volume]

---

####  Exemple
```python
from DataManager import DataManager

manager = DataManager(path="data/", 
                      market = "SOL/USDT",
                      timeframe = "1m",
                      since = 1540991600,
                      limit = 10000)
manager.load()
print(manager.main_dataframe)
```

#### Output

<img src="https://cdn.discordapp.com/attachments/901790872033714216/960244524351914024/unknown.png" alt="drawing" width="800"/>
