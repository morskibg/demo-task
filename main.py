import os
import re
from typing import Union, List, Dict
import pandas as pd
from pathlib import Path
from time import perf_counter
import asyncio
import aiofiles
import aiohttp
from bs4 import BeautifulSoup
import platform
from aiohttp.client_exceptions import ClientConnectorError, InvalidURL

if platform.system()=='Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

WORDS = ['team','uniberg']

def get_urls() -> List[str]:
    """Read urls from text file in root directory named "urls.txt".
    Returns:
        list[str]: read urls from text file
    """    """"""
    url_file_path = Path(os.getcwd()).joinpath('urls.txt')    
    with open(url_file_path, mode='r') as f:
        urls = [line.strip() for line in f.readlines()]
        return urls

async def count_words(soup: BeautifulSoup, word: str, url: str) -> Dict[str, Union[str,int]]:
    """Using soup object to match all occurrences of selected word in html text
    with regex.
    Args:
        soup (BeautifulSoup): soup object
        word (str): word to count occurrences for
        url (str): url of scraped html
    Returns:
        dict[Union[str,int]]: dict of word's occurrences
    """    
    html_text = soup.get_text()       
    match_results = re.findall(word, html_text, re.IGNORECASE)
    count_dict = {'url':url,'word': word, 'count': len(match_results)}
    return count_dict

async def save_data(df: pd.DataFrame) -> None:
    """Asynchronous saving aggregated data to text file.
    Args:
        df (pd.DataFrame): dataframe with data from every url with word's occurrences count 
    """    
    result_file_path = Path(os.getcwd()).joinpath('result.txt') 
    words = df['word'].unique()
    urls = df['url'].unique()
    pivot_df = df.pivot(index = 'word', columns = 'url', values=['total_words','unique_words','count'])
    lines = []
    for word in words:
        lines.append(f'Occurrences of word "{word}":')
        [lines.append(f'- {url} => {pivot_df.loc[word,"count"][url]}') for url in urls]    
        lines.append(f'- Total count for word "{word}" in all urls => {pivot_df.loc[word,"count"].sum()}')
    lines.append(f'* Total count of all words in all urls => {pivot_df.loc[word,"total_words"].sum()}')
    lines.append(f'* Total count of unique words in all urls => {pivot_df.loc[word,"unique_words"].sum()}')
    async with aiofiles.open(result_file_path, mode='a') as f:
        await f.write(('\n'.join(lines)))

async def get_aggr_data(soup: BeautifulSoup, url: str) -> Dict[str, Union[str,int]]:
    """Count total words and total unique words from paesed html text.
    Args:
        soup (BeautifulSoup): soup object
        url (str): source url
    Returns:
        dict[Union[str,int]]: dict with counted data
    """   
    all_words = [x for x in soup.get_text(" ", strip=True).lower().split() if x.isalpha()]
    aggr_dict = {
        'url':url,
        'total_words':len(all_words), 
        'unique_words':len(set(all_words))
    }
    return aggr_dict

async def fetch_data(url: str, session: aiohttp.ClientSession, words: List[str]) -> pd.DataFrame: 
    """Asynchronous fetching data from url and running async word's counting.
    Args:
        url (str): source url
        session (aiohttp.ClientSession): async aiohttp session object
        words (list[str]): words occurrences to search for in returned html from url
    Returns:
        pd.DataFrame: dataframe with counted data
    """       
    async with session.get(url) as resp:
        body = await resp.text()
        soup = BeautifulSoup(body, 'html.parser')
        aggr_dict = await get_aggr_data(soup, url)
        count_dict = await asyncio.gather(*[count_words(soup, word, url) for word in words])
        aggr_df = pd.DataFrame([aggr_dict]) 
        count_df = pd.DataFrame(count_dict)
        url_stat_df = pd.merge(aggr_df, count_df, on = 'url')
        return url_stat_df       
  
async def fetch_all(urls: List[str], session: aiohttp.ClientSession, words: List[str]) -> pd.DataFrame:
    """Initiating async fetching for every specified in text file url.
    Args:
        urls (list[str]): target urls from text file.
        session (aiohttp.ClientSession): async aiohttp session object
        words (list[str]): words occurrences to search for in returned html from url.
    Returns:
        pd.DataFrame: concatenated full dataframe from all urls
    """     
    cooroutines = [fetch_data(url, session, words) for url in urls]    
    return pd.concat(await asyncio.gather(*cooroutines), ignore_index=True) 

async def main(urls: List[str], words: List[str]) -> None:
    """Main async function, which creating async aiohttp session and starting 
    fetching data from all urls and saving to text file.
    Args:
        urls (list[str]):target urls from text file.
        words (list[str]): words occurrences to search for in returned html from url
    """    
    start_time = perf_counter()
    async with aiohttp.ClientSession() as session:
        final_df = await fetch_all(urls, session, words)        
        await save_data(final_df)

    time_difference = perf_counter() - start_time
    print(f'Elapsed time: {round(time_difference, 4)} seconds.')

if __name__ == '__main__':
    try:
        urls = get_urls() 
        assert len(urls) > 0        
    except FileNotFoundError as ex:
        print(f'Please provide valid "ursl.txt" file.\n{ex}')
    except AssertionError as ex:
        print('File "ursl.txt" is empty!')
    else:
        try:
            with open(Path(os.getcwd()).joinpath('result.txt'), "r+") as f:
                f.truncate(0)
        except FileNotFoundError as ex:
            pass
        try: 
            asyncio.run(main(urls, WORDS))
        except InvalidURL as ex:
            print(f'There is an invalid url -> "{ex}" in "ursl.txt" file.')
        except ClientConnectorError as ex:
            print(f'Connection error\n{ex}')
        except Exception as ex:
            print(f'General exception\n{ex}')
            

