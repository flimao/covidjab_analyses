import os
from datetime import datetime as dt
from time import sleep
import re
from playwright.sync_api import sync_playwright, expect
import pandas as pd
from tqdm.contrib import itertools

# dotenv incantations
from dotenv import load_dotenv, find_dotenv

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()

# load up the entries as environment variables
load_dotenv(dotenv_path)

REGISTRY_URL = os.getenv('CIVILREGISTRY_URL')

# compile regex for unwanted dropdown options in page
UNWANTED_LIST_OPTS = re.compile('empty|query|busca|od(o|a)s')


def trim_list_unwanted(
    raw_list: list, 
    unwanted_opts: re.Pattern = UNWANTED_LIST_OPTS
) -> list:
    """ Takes list and returns filtered list based in regex of unwanted opts
    """
    ret_list = [
        opt.strip()
        for opt in raw_list
        if unwanted_opts.search(opt) is None
    ]

    return ret_list

def process_births_df(df: pd.DataFrame) -> pd.DataFrame:
    """ take dataframe from civil registry webpage and 
        process it into something usable for analysis
    """
    # copy df so it doesn't modify input instance
    df = df.copy()

    # rename columns
    df.columns = ['state', 'registered_births']

    # change type
    df['state'] = df['state'].astype('category')
    df['registered_births'] = df['registered_births'].astype(int)

    # add year-month column
    months = {
        'janeiro': 1,
        'fevereiro': 2,
        'março': 3,
        'marco': 3,
        'abril': 4,
        'maio': 5,
        'junho': 6,
        'julho': 7,
        'agosto': 8,
        'setembro': 9,
        'outubro': 10,
        'novembro': 11,
        'dezembro': 12
    }
    df['month'] = dt(int(year), months[month.lower()], 1)

    # reorder columns
    new_order = ['month', 'state', 'registered_births']
    df = df.reindex(columns = new_order)

    # reset index
    df = df.reset_index(drop = True)

    return df

with sync_playwright() as p:
    with p.chromium.launch(headless = False) as browser:
        page = browser.new_page()
        page.goto(REGISTRY_URL)

        # get natality radio
        natality_radio = page.get_by_label('Nascimentos').locator('xpath=../input')

        # select natality radio
        natality_radio.dispatch_event('click') # why not .click()? doesnt work on this page, other tag on top

        # get year dropdown
        dd_year_input = page.get_by_placeholder('selecione o ano')
        dd_year = dd_year_input.locator('../..')
        # find all options on 'year' dropdown
        year_options_nodes = dd_year.locator('xpath=.//li')
        year_options_raw = year_options_nodes.all_inner_texts()

        # process year options
        year_options = trim_list_unwanted(raw_list = year_options_raw)

        # get month dropdown
        dd_month_input = page.get_by_placeholder('selecione o mês')
        dd_month = dd_month_input.locator('../..')
        # find all options on 'month' dropdown
        month_options_nodes = dd_month.locator('xpath=.//li')
        month_options_raw = month_options_nodes.all_inner_texts()

        month_options = trim_list_unwanted(month_options_raw)

        # get state dropdown
        dd_state_input = page.get_by_placeholder('selecione um estado')
        dd_state = dd_state_input.locator('../..')
        # find all options on 'state' dropdown
        state_options_nodes = dd_state.locator('xpath=.//li')
        state_options_raw = state_options_nodes.all_inner_texts()

        state_options = trim_list_unwanted(state_options_raw)

        # find search button
        search_button = page.get_by_role('button', name = 'Pesquisar')

        # find results table
        results_table = page.get_by_role('table')

        ## LOOP
        # set up loop
        reg_dfs = None

        # loop through combination of options of year-month
        for year, month in itertools.product(
            year_options, month_options, desc='Years and months'
        ):
            # set year
            dd_year.click()
            dd_year_input.fill(year)
            page.keyboard.press('Enter')

            # set month
            dd_month.click()
            dd_month_input.fill(month)
            page.keyboard.press('Enter')
            
            # set state
            # dd_state.click()
            # dd_state_input.fill(state_options[-1])
            # page.keyboard.press('Enter')
            
            # go!
            search_button.click()

            # wait for results to load
            expect(results_table).to_be_visible()
            # if there are no results, skip
            if 'resultados a serem exibidos' in results_table.inner_text():
                continue

            # save outer html to string
            # https://stackoverflow.com/questions/70891225/how-to-get-outer-html-from-python-playwright-locator-object
            results_html = results_table.evaluate("el => el.outerHTML")

            # import html string into pandas dataframe
            df_raw = pd.read_html(results_html)[0]
            
            # process df
            df = process_births_df(df_raw)

            if reg_dfs is not None:
                reg_dfs = pd.concat([reg_dfs, df])
            else:
                reg_dfs = df

        # reg_dfs.info()
        # print(reg_dfs)
        savepath = os.path.abspath(r'covidjab_natality/data/processed')
        savefile = os.path.join(savepath, 'brazil_natality_state_monthly.parquet')
        reg_dfs.to_parquet(savefile)

        sleep(3)
