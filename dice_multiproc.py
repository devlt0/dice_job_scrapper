import pickle
import traceback
from os import getpid
from os.path import exists

from datetime import datetime
from time import sleep, time

from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from threading import current_thread  # b/c we print thread name out with proc >_<, possibly remove both ToDo investiage

import pandas

from selenium import webdriver

from selenium.common.exceptions import \
    TimeoutException, NoSuchElementException, ElementNotInteractableException, \
    ElementClickInterceptedException, StaleElementReferenceException
#from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.chrome.options import Options




RESPG_ITER_STUB = "!~!PG_ITER!~!"
SW_ENG_REM_URL = "https://www.dice.com/jobs?q=software%20engineer&countryCode=US&radius=30&radiusUnit=mi&page=1&pageSize=20&filters.isRemote=true&language=en&page="+RESPG_ITER_STUB
PY_REM_URL = "https://www.dice.com/jobs?q=python&radius=30&radiusUnit=mi&pageSize=20&filters.isRemote=true&language=en&page="+RESPG_ITER_STUB
SQL_REM_URL = "https://www.dice.com/jobs?q=SQL&radius=30&radiusUnit=mi&pageSize=20&filters.isRemote=true&language=en&page="+RESPG_ITER_STUB # 1500  2622.48 seconds
SW_TEST_REM_URL = "https://www.dice.com/jobs?q=software%20test&countryCode=US&radius=30&radiusUnit=mi&page=1&pageSize=20&filters.isRemote=true&language=en&page="+RESPG_ITER_STUB
SW_REM_URL = "https://www.dice.com/jobs?q=software&countryCode=US&radius=30&radiusUnit=mi&page=1&pageSize=20&filters.isRemote=true&language=en&page="+RESPG_ITER_STUB
SW_DEV_REM_URL = "https://www.dice.com/jobs?q=software%20developer&countryCode=US&radius=30&radiusUnit=mi&page=1&pageSize=20&filters.isRemote=true&language=en&page="+RESPG_ITER_STUB
ALL_REM_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.isRemote=true&language=en&page="+RESPG_ITER_STUB
ALL_LAST_7D_REM_URL = "https://www.dice.com/jobs?radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=Remote&language=en&page="+RESPG_ITER_STUB
ALL_LAST_7D_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&language=en&page="+RESPG_ITER_STUB
ALL_LAST_14D_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=FOURTEEN&language=en&page="+RESPG_ITER_STUB
ALL_LAST_30D_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&language=en&page="+RESPG_ITER_STUB
LAST_7D_HYBRID_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=Hybrid&language=en&page="+RESPG_ITER_STUB
LAST_7D_REMOTE_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=Remote&language=en&page="+RESPG_ITER_STUB
LAST_7D_HYBRID_OR_REMOTE_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=Hybrid%7CRemote&language=en&page="+RESPG_ITER_STUB
# 5K COMBINED LAST 7D HYBRID OR REMOTE
LAST_7D_ONSITE_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=On-Site&language=en&page="+RESPG_ITER_STUB
# 29K
LAST_7D_ONSITE_RECRUITER_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=On-Site&filters.employerType=Recruiter&language=en&page="+RESPG_ITER_STUB
LAST_7D_ONSITE_DIRECT_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=On-Site&filters.employerType=Direct%20Hire&language=en&page="+RESPG_ITER_STUB
ALL_REMOTE_EASYAPPLY_DICE_URL = "https://www.dice.com/jobs?radius=30&radiusUnit=mi&pageSize=20&filters.workplaceTypes=Remote&filters.easyApply=true&language=en&page="+RESPG_ITER_STUB
LAST_7D_REMOTE_EASYAPPLY_DICE_URL = "https://www.dice.com/jobs?radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=SEVEN&filters.workplaceTypes=Remote&filters.easyApply=true&language=en&page="+RESPG_ITER_STUB
MAX_DICE_PG_20_RES_PG = 500 # 500 pages @ 20 results per page = 10k results
DICE_7500RES_20_RES_PG = 375  # 375p @ 20res/p = 7500 res
# dice while having the data won't let you look furter back than 10k results

# tuples (url to scan, how many pages to scan)
comprehensive_combo = [ \
                       (LAST_7D_ONSITE_DIRECT_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       (LAST_7D_ONSITE_RECRUITER_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       (LAST_7D_HYBRID_OR_REMOTE_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       #(LAST_7D_REMOTE_EASYAPPLY_DICE_URL, DICE_7500RES_20_RES_PG)
                      ]

LAST_30D_ONSITE_RECRUITER_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=THIRTY&filters.workplaceTypes=On-Site&filters.employerType=Recruiter&language=en&page="+RESPG_ITER_STUB
LAST_30D_ONSITE_DIRECT_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=THIRTY&filters.workplaceTypes=On-Site&filters.employerType=Direct%20Hire&language=en&page="+RESPG_ITER_STUB
LAST_30D_HYBRID_OR_REMOTE_DICE_URL = "https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&pageSize=20&filters.postedDate=THIRTY&filters.workplaceTypes=Hybrid%7CRemote&language=en&page="+RESPG_ITER_STUB

comprehensive_combo_7D = [ \
                       (LAST_7D_ONSITE_DIRECT_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       (LAST_7D_ONSITE_RECRUITER_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       (LAST_7D_HYBRID_OR_REMOTE_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       #(LAST_7D_REMOTE_EASYAPPLY_DICE_URL, DICE_7500RES_20_RES_PG)
                      ]


comprehensive_combo_30D = [ \
                       (LAST_30D_ONSITE_DIRECT_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       (LAST_30D_ONSITE_RECRUITER_DICE_URL, MAX_DICE_PG_20_RES_PG),
                       (LAST_30D_HYBRID_OR_REMOTE_DICE_URL, MAX_DICE_PG_20_RES_PG),
                      ]




remote_combo =[(LAST_7D_HYBRID_OR_REMOTE_DICE_URL, MAX_DICE_PG_20_RES_PG)]
backup_combo = [(ALL_LAST_7D_DICE_URL, MAX_DICE_PG_20_RES_PG)]
auto_apply_remote_combo = [(ALL_REMOTE_EASYAPPLY_DICE_URL, MAX_DICE_PG_20_RES_PG)]

all_combos = []
all_combos.extend(comprehensive_combo)
all_combos.extend(backup_combo)

'''
# make up missing
comprehensive_combo = [ \
                       (LAST_7D_HYBRID_OR_REMOTE_DICE_URL, int(MAX_DICE_PG_20_RES_PG/2)+1 ),
                      ]
'''
# expects
## 10k res onsite direct,
## 10k res onsite recruiter
## 5k res hybrid [any]
### this will skip < 100 onsite other

# ToDo double check windows/webdriver instances actually get closed/exited
#  webdriver is done in with context so should auto exit at end...
# Define the simplified retry decorator
def retry(stop_max_attempt_number=3, wait_fixed=0):
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < stop_max_attempt_number:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= stop_max_attempt_number:
                        raise
                    print(f"Attempt {attempt} failed. Retrying...")
                    sleep(wait_fixed)

        return wrapper
    return decorator



#@profile
def get_datetimestamp_str(str_format:str='%Y-%m-%d__%H-%M-%S')->str:
    '''
    '%Y-%m-%d__%H-%M-%S-%f'
    Year-month-day__Hour-Minute-Seconds-Microseconds
    '''
    #'%H-%M-%S_%d-%m-%Y'
    current_time = datetime.now()
    formatted_time = current_time.strftime(str_format)
    return formatted_time



#@profile
def gen_get_empty_df(col_titles:list=['title', 'company_name', 'location', 'employment_type', 'salary', 'salary_alt', 'skills', 'date_posted', 'date_posted_alt', 'dice_job_url', 'company_url', 'full_job_desc', 'recruiter']):
    base_df = pandas.DataFrame(columns=col_titles)
    return base_df.copy()  # copy maybe not needed



#@profile
def scrape_job_page(pg_num:int, url_to_use:str)->list:
    # ToDo break up function into subfunctions
    df_jobs = gen_get_empty_df() #pd.DataFrame(columns=['title', 'company_name', 'location', 'employment_type', 'salary', 'salary_alt', 'skills', 'date_posted', 'date_posted_alt', 'dice_job_url', 'company_url', 'full_job_desc'])
    jobs_listodicts = []
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument("--window-size=1920,1080");
    chromeOptions.add_argument("--disable-gpu");
    chromeOptions.add_argument("--disable-cache");
    chromeOptions.add_argument("--disable-extensions");
    chromeOptions.add_argument("--proxy-server='direct://'");
    chromeOptions.add_argument("--proxy-bypass-list=*");
    chromeOptions.add_argument("--headless");



    dice_url_to_use = url_to_use
    # since posting on dice are for 30days, scanning all is really scanning last 30days

    job_list_box_height = 214  # personal investigation on both firefox and chrome with builtin browser dev tools
    driver = None
    try:
        with webdriver.Chrome(options=chromeOptions) as driver:
            driver.delete_all_cookies()

            #actChain   = ActionChains(driver)
            wait_time  = 5
            webwait    = WebDriverWait(driver, wait_time)
            #MAX_RESULTS = 111  # to test out and get a feel of time to scan a page or avg time per result
            START_PG = 1
            res_ctr     = 0
            is_first_page = True if pg_num == 1 else False

            print(f"Thread {current_thread().name} on page {pg_num}")
            url = dice_url_to_use.replace(RESPG_ITER_STUB, str(pg_num) )
            #//*[@id="totalJobCount"]

            try:
                driver.get(url)
                sleep(3.33) # loading page can take a bit
            except Exception as e:
                print (e)

            job_listings = []
            retry_limit = 3
            retry_ctr = 0
            while job_listings == []\
            and retry_ctr < retry_limit:
                try:
                    job_listings = driver.find_elements(By.XPATH, '//div[contains(@class, "search-card")]')
                except Exception as e:
                    print(e)
                retry_ctr += 1
                sleep(1)

            if is_first_page:
                #total_res = driver.find_element_by_xpath('//*[@id="totalJobCount"]').text.strip()
                total_res = ''
                while total_res == '':
                    try:
                        #total_res = driver.find_element_by_xpath('//span[@id="totalJobCount"]').text.strip()  #contains(@id, "totalJobCount")]').text.strip()
                        total_res = driver.find_element(By.XPATH, '//span[@id="totalJobCount"]').text.strip()
                        print(f"Total # jobs found to scrap: {total_res}")
                        print(f"total res type {type(total_res)}")
                    except Exception as e:
                        print("Failed to get total # of jobs, sleeping and retrying...")
                        sleep(1)
                is_first_page = False


            JOB_ITER_STUB = "!~!JOB_ITER!~!"

            #job_listings = driver.find_elements(By.XPATH, '//div[contains(@class, "search-card")]')
            MAX_RES_PER_PG = 20  # jobsite specific, iirc dice can change this but default is 20/page
            # worth noting no faux job listing that are ads such as on wellfound/angellist
            result_iter = 0 # per page
            # need to improve this or have separate process that goes back and chks if details are there
            for job in job_listings:
                scroll_multiplier = 1 if (result_iter > 0) else 0
                driver.execute_script("window.scrollBy(0,"+str(job_list_box_height * scroll_multiplier)+")","")

                sleep(0.5)
                try:
                    curJobDict = {}
                    if result_iter >= MAX_RES_PER_PG:
                        print(f"\n\n\tProcessed {res_ctr} results, continuing to next page...")
                        break


                    try:
                        company          = job.find_element(By.XPATH, './/a[contains(@data-cy, "search-result-company-name")]').text
                    except Exception as e:
                        company = ""

                    try:
                        location         = job.find_element(By.XPATH, './/span[contains(@data-cy, "search-result-location")]').text   #"jobCard-location")]').text #job.find_element_by_css_selector('.location span').text
                    except Exception as e:
                        location = ""

                    try:
                        posted_date      = job.find_element(By.XPATH, './/span[contains(@class, "posted-date")]').text  #job.find_element_by_css_selector('.posted-date').get_attribute('title')
                    except Exception as e:
                        posted_date = ""


                    salary = None
                    # dice doesn't include salary in top lvl info
                    #job.find_element_by_xpath().text # base_title_elem.text  #job.find_element_by_css_selector('.card-header a').text

                    # click + switch + process new tab
                    ## randomly will get stuck unable to click here and not sure what to do, the while loop seemingly didn't scroll down, would scroll down then back up to where it startup

                    clicked_into_job_details = False
                    try:
                        base_title_elem  = job.find_element(By.XPATH, './/a[contains(@class, "card-title-link")]')
                        title            = base_title_elem.text
                        base_title_elem.click()
                        clicked_into_job_details = True
                    except Exception as e:
                        title = ""
                        #print(e)
                        #print("Trying do while loop to find next job title")
                    if not clicked_into_job_details:
                        try:
                            base_title_elem  = job.find_element(By.XPATH, './/a[contains(@class, "card-title-link")]')
                            driver.execute_script('arguments[0].click()', base_title_elem)  # stack overflow magic sauce ???
                            clicked_into_job_details = True
                        except Exception as e:
                            #print(e)
                            pass


                    retry_limit = 5
                    retry_ctr = 0
                    while not clicked_into_job_details  and retry_ctr < retry_limit:
                        try:
                            base_title_elem  = job.find_element(By.XPATH, './/a[contains(@class, "card-title-link")]')
                            #print(f"Current element title: {base_title_elem.text}")
                            base_title_elem.click()
                            clicked_into_job_details = True
                        except Exception as e:
                            #print(e)
                            #print(f"Scrolling down and trying again to click into element title: {base_title_elem.text}")
                            # b/c selenium can "see" the text without having the element be in viewable range to click
                            if retry_ctr % 2 == 0:
                                driver.execute_script("arguments[0].scrollIntoView(true);", base_title_elem)
                            # since scrollIntoView and scrollBy will sometimes exhibit peculiar behavior that the other wont
                            # ie- scroll works normally at top level, but scroll will scroll specified amount and bounce back to start here
                            #     ?suspected? scroll issue to be related to binding box of outside element but given scrolling via hard pixel doesn't make sense to be this

                            else:
                                driver.execute_script("window.scrollBy(0,100)","")
                        retry_ctr += 1

                        sleep(1)

                    num_window_handles = len(driver.window_handles)
                    # ToDo convert nested if to guard clause
                    if clicked_into_job_details and num_window_handles > 1:
                        sleep(0.5)
                        driver.switch_to.window(driver.window_handles[1])
                        sleep(0.5)


                        url_job_detailed = driver.current_url
                        # ToDo - update to have chk that url_job_detailed starts with "https://www.dice.com/job-detail/"
                        #  if not- ignore trying to process- maybe later collect urls that aren't dice and see what the major ones are / how to proc
                        # ToDo convert the try/except to wrapper for value assignment?- pull to function so each 4 link block becomes 1
                        try:
                            url_company = driver.find_element(By.XPATH, './/a[contains(@data-cy, "companyNameLink")]').get_attribute("href")
                        except Exception as e:
                            url_company = ""

                        try:
                            time_ago = driver.find_element(By.XPATH, './/span[contains(@id, "timeAgo")]').text
                        except Exception as e:
                            time_ago = ""

                        # intentionally standardized missing to be easier to filter out later on if necessary
                        try:
                            salary = driver.find_element(By.XPATH, './/span[contains(@id, "payChip")]').text
                        except Exception as e:
                            #print(e)
                            salary = ""

                        try:
                            employ_type = driver.find_element(By.XPATH, './/span[contains(@id, "employmentDetailChip")]').text
                        except Exception as e:
                            #print(e)
                            employ_type = ""

                        try:
                            skills = driver.find_element(By.XPATH, './/div[contains(@data-cy, "skillsList")]').text
                        except Exception as e:
                            #print(e)
                            skills = ""

                        try:
                            recruiter_name = driver.find_element(By.XPATH, '//p[contains(@data-cy, "recruiterName")]').text
                        except Exception as e:
                            recruiter_name = ""
                            #print(e) # ToDo change to log
                            pass

                        easy_apply_button_exists = "easyApply%3Dtrue" in url_job_detailed

                        # ToDo pull retry logic out to own function or use something builtin python

                        # loop til we can click to get full job description # forgiveness first principle
                        full_desc_expanded = False
                        # since its possible to have short desc w/o job description toggle, try 3 times then bail and pretend its all cool
                        # checking results of previous posts that stumped this works- doesn't enter infinite loop o doom
                        retry_limit = 3
                        retry_ctr = 0
                        while not full_desc_expanded and retry_ctr < retry_limit:
                            try:
                                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                desc_toggle_elem = driver.find_element(By.XPATH, './/button[contains(@id, "descriptionToggle")]')
                                driver.execute_script('arguments[0].click()', desc_toggle_elem)  # stack overflow magic sauce ???
                                full_desc_expanded = True
                            except Exception as e:
                                #print(f"Magic sauce failed on {url_job_detailed}")
                                #print(e)
                                pass
                            sleep(0.5)
                            if not full_desc_expanded:
                                try:
                                    driver.find_element(By.XPATH, './/button[contains(@id, "descriptionToggle")]').click()
                                    full_desc_expanded = True
                                except Exception as e:
                                    #print(e)
                                    print("Scrolling and retrying to click button with id descriptionToggle")
                                    driver.execute_script("window.scrollBy(0,"+str(job_list_box_height)+")","")
                                    sleep(1)
                                    #driver.find_element(By.XPATH, './/button[contains(@id, "descriptionToggle")]').click()
                            retry_ctr += 1

                        sleep(0.5)
                        try:
                            full_job_desc = driver.find_element(By.XPATH, './/section[contains(@class, "job-description")]').text
                        except Exception as e:
                            #print(e)
                            full_job_desc = ""

                        driver.close()
                        sleep(0.5)
                        driver.switch_to.window(driver.window_handles[0])
                        sleep(0.5)

                        # Print job information
                        # skills is newline delimited list as string
                        ## only save job if we can click in - otherwise no skill, no salary
                        #df_jobs = pd.DataFrame(columns=['title', 'company_name', 'location', 'employment_type', 'salary', 'salary_alt', 'skills', 'date_posted', 'date_posted_alt', 'dice_job_url', 'company_url', 'full_job_desc'])
                        now = datetime.now()
                        cur_date = now.strftime("%Y-%m-%d")
                        cur_time = now.strftime("%H:%M")
                        # silly question- why waste time with intermediate variables or more readability?
                        curJobDict['title']         = title
                        curJobDict['company_name']  = company
                        curJobDict['location']      = location
                        curJobDict['date_posted']   = posted_date

                        curJobDict['employment_type'] = employ_type
                        curJobDict['salary']        = salary
                        #curJobDict['salary_alt']    = salary_alt
                        curJobDict['skills']        = skills

                        curJobDict['date_posted_alt'] = time_ago
                        curJobDict['dice_job_url']  = url_job_detailed
                        curJobDict['company_url']    = url_company
                        curJobDict['full_job_desc']  = full_job_desc
                        curJobDict['recruiter']  = recruiter_name
                        curJobDict['date_scraped']  = cur_date
                        curJobDict['time_scraped']  = cur_time
                        curJobDict['can_easy_apply']  = easy_apply_button_exists


                        if (0):
                            print(f"Title:  {title}\n\tCompany: {company}\n\tLocation: {location}" +
                              f"\n\tEmployment Type:  {employ_type}" +
                              f"\n\tSalary:  {salary}" +
                              f"\n\tSkills:\n{skills}" +
                              f"\n\tDate Posted:  {posted_date}" +
                              f"\n\tPosted Time Ago:  {time_ago}" +
                              f"\n\tDetailed Link:  {url_job_detailed}" +
                              f"\n\tCompany Link:  {url_company}" +
                              f"\n\tRecruiter Name:  {recruiter_name}" +
                              f"\n\tThread: {current_thread().name}"
                            )
                            # print( f"\n\tFull Job Descrip:  {full_job_desc}" )


                        #df_jobs.loc[len(df_jobs)] = curJobDict
                        jobs_listodicts.append(curJobDict)
                        print(f"Thread {current_thread().name} Processid {getpid()} - Processed job # {res_ctr} out of {MAX_RES_PER_PG} on page {pg_num}\n\n\n")

                        result_iter += 1
                        res_ctr+=1
                        # close or exit?
                    else:
                        print(f"Thread {current_thread().name} Processid {getpid()} - Skipped processing job # {res_ctr} on page {pg_num} with title {title} due to being unable to click into details.\n\n\n")

                except Exception as e:
                    print(e)

            driver.close() # close original window
    except Exception as e:
        print(e)
        # ToDo investigate if try/except wrapper around with + overkill_cleanup makes a diff
        # kind of crazy to be hitting 30gb of ram use
    # ToDo debug why nothing prints out now- but seemingly still works
    overkill_cleanup(given_webdriver=driver)
    #return df_jobs
    return jobs_listodicts


#@profile
def overkill_cleanup(given_webdriver:webdriver.Chrome,
                     pass_excep_silently:bool=True):
    '''
    try:
        num_window_handles = len(given_webdriver.window_handles)
        for x in range(num_window_handles):



                # ToDo convert nested if to guard clause
                if clicked_into_job_details and num_window_handles > 1:
                    sleep(0.5)
                    driver.switch_to.window(driver.window_handles[1])
                    sleep(0.5)
        driver.close()
    '''
    # ToDo investigate if closing all open windows before killing off driver makes a meaningful difference
    #  meaningful in terms of causing bugs, faster/longer

    try:
        if given_webdriver is not None:
            given_webdriver.quit()
    except Exception as e:
        if not pass_excep_silently:
            print(e)




# ToDo - merge or abstract common logic from multiproc and multi thread scrap func
#@profile
def scrape_job_listings_multiproc(process_cnt:int = 8,
                                  chunk_size:int = 1000,
                                  num_pgs_to_scan:int=1000,
                                  url_to_scrape:str="",
                                  _start_page:int=0) -> int:
    save_ctr = 1
    cur_save_threshold = (save_ctr * chunk_size) - 1
    num_jobs_processed = 0
    master_df = gen_get_empty_df()
    master_job_details_listodicts = []
    num_job_listings_per_page = 20
    num_jobs_scanned = 0
    chunk_size_as_page = int( chunk_size / num_job_listings_per_page )
    retry_limit = 3
    retry_ctr = 0

    with Pool(process_cnt) as pool:

        # no sense in loading entire list if going to be chunk saving, so only load as much as well save at a time
        for chunk_start in range(_start_page, num_pgs_to_scan, chunk_size_as_page):
            #chunk_end = min(chunk_start + chunk_size, num_job_urls_to_process)
            #chunk_urls = job_df[url_col].iloc[chunk_start:chunk_end].tolist()

            #results = [pool.apply_async(job_detail_scrap, args=(job_url,)) for job_url in chunk_urls]
            results = [pool.apply_async(scrape_job_page, args=(pg_iter+1,url_to_scrape)) for pg_iter in range(chunk_start, (chunk_start+chunk_size_as_page) )]


        # da silly way.... NOT silly!!! map blocks ability to save intermediate results
        # meaning you get all or nothing runs on 20k+ results 24hrs + run at risk
        # for job desc pull 1k chunk size resulted in saving ~50-55min
        #results = [pool.apply_async(job_detail_scrap, args=(job_url,)) for job_url in job_df[url_col]]

            for result in results:
                job_details = result.get()  # Get the result from the async task

                #master_df = pandas.concat([master_df, job_details], ignore_index=True)
                #--> was using #master_df.loc[len(master_df)] = job_details
                #master_job_details_listodicts.append(job_details)

                master_job_details_listodicts.extend(job_details) # since job_details will be list of 20 dictionaries from pg scanned
                # ToDo save intermediate data in another format and convert to dataframe only when output to excel

                num_jobs_scanned = len(master_df)

            # now build our df
            master_df = pandas.DataFrame(master_job_details_listodicts)
            # no longer if chk against threshold given we chunked out what we want to save
            saved_worked = save_results(given_df=master_df, url_variable=url_to_scrape) # given only use df for saving should move it into func <----  #ToDo  that stuff
            while not saved_worked\
            and retry_ctr < retry_limit:
                retry_ctr += 1
                saved_worked = save_results(given_df=master_df, url_variable=url_to_scrape)

            if saved_worked:
                num_jobs_processed += num_jobs_scanned
                num_jobs_scanned = 0
            else:
                #raise Exception(f"save not working, failed @ time: {get_datetimestamp_str()}")
                print(f"save not working, failed @ time: {get_datetimestamp_str()}")
            del master_job_details_listodicts
            del master_df
            master_job_details_listodicts = []
            master_df =  gen_get_empty_df()

            # skeptical but supposed this could help save on ram? Given it gets immediately reassigned not sure
            del results


        num_jobs_scanned = len(master_df)
        retry_ctr = 0
        if num_jobs_scanned > 0:
            saved_res = save_results(given_df=master_df, url_variable=url_to_scrape)
            still_retry = retry_ctr < retry_limit
            while not saved_res and still_retry:
                retry_ctr += 1
                saved_res = save_results(given_df=master_df, url_variable=url_to_scrape)
                still_retry = retry_ctr < retry_limit

            num_jobs_processed += num_jobs_scanned


    return num_jobs_processed



# ToDo figure out if want to keep multithread version given multiple local tests showed multiproc consistently ~2x faster
# def scrape_job_listings_multithread(threadpool_cnt:int, num_pgs_to_scan:int, _start_page:int=0): #->int:


#@retry
# ToDo fix retry implementation or import lib
#@profile

# double chk this doesn't need to be annotated with @async for multiproc

# ToDo need to make functions called from pool async otherwise run into issue?
def save_results(given_df:pandas.DataFrame=None, as_excel:bool=True, as_feather:bool=False, url_variable:str="") -> bool:
    usable_df = given_df.copy()  # b/c mutable parameters in python are fun
    num_jobs_scanned = len(usable_df)
    url_var_name = get_global_variable_name(url_variable)
    base_name = f"{url_var_name}---{get_datetimestamp_str()}---{num_jobs_scanned}"
    saved_data = False
    pickle_saved = False
    excel_saved = False
    ctr = 1
    if as_excel:
        try:
            target_fname = base_name + ".xlsx"
            while exists(target_fname):   # need to update this to chk if two files with same name exist, only last alt file written, and are same size, if so- auto exist as file written
                ctr += 1
                target_fname = base_name + f"---alt{ctr}.xlsx"  # also pretty sure this should be async given called from multiple processes >_<
            usable_df.to_excel(target_fname, index=False)  # Save DataFrame to Excel
            excel_saved = True
            saved_data = True
        except Exception as e:
            saved_data = False
            print(e)

    ## ToDo add feather implementation + update saved_data logic

    return saved_data


# why was this needed again?  guessing not using async with multi proc/thread
def get_global_variable_name(variable):
    variable_name = ""
    try:
        variable_name = [name for name, value in globals().items() if value is variable][0]
    except Exception as e:
        print(e)
    return variable_name
    #print(f"Variable name using globals(): {variable_name}")



# ToDo
# add function that would provide popup checkbox to select the files to merge after running
# likely move this to separate utility file or figure out better way / cfg file for files to inc in merge // like columns


#Total # jobs found to scrap: 1,317 # 65 for all
# 1300 jobs, 4 threads, 2280sec --> 1.76s/job scraped (including opening new tab)
# 5923s but failed output, 175pages @ 20 per page --> 3500 listings --> 1.69sec/scraped job listing
# can round up to 2s/job scraped providing ~11% buffer on low end estimates
# 3960 sw test  5797.66 seconds --> 1.464s/job scrap
# 3528 for software alone, adding programmer, developer, engineer seemingly reduces but without having spare data set for comparison, sol
# Elapsed time: 4879.48 seconds   --> 1.383s/job
# 7722 for all remote jobs on dice -- meaning software makes up about half of all remote sh*t
# sw dev -  5716.97 seconds - 3500
# dice all software remote          -  2.7k
# dice all remote - 7800            -  7.8k -- 8460.76 --> 1.085s/job scrapped i7 3770 (4c/8t) 3.4ghz base clock 7 thread pool // ~141min aka 2.35hrs aka 2hrs 21min
# dice all jobs regardless location - 57.6k -- running on 4900h (8c/16t) 3.3ghz base clock

# 2800 pages for all dice --> 56k jobs
# on 8proc est 12.5hrs so starting at quarter to 2am, likely finish after 2 before 3pm same day
MAX_NUM_DICE_PGS = 500 # = 10k listings
# given above- means when don't scan we potentially miss data with no ability to recover outside more searches

if __name__ == '__main__':

    start_time = time()
    #num_multi = 8
    #num_pages_to_read = 32
    try:
        # ToDo pull options for dice urls here, possibly enum with default
        # num pgs to scan - each page has 20 results so it's x20, usually default 1k for 20k for last week
        # avg ~15min per 1k scraped
        '''
        url_to_use = ALL_LAST_7D_DICE_URL
        num_jobs_scanned = scrape_job_listings_multiproc(
                                              process_cnt = 6,
                                              num_pgs_to_scan = 500,
                                              _start_page = 0
                                             )
        '''

        # really want 850 to get 17k jobs from last 7days on dice // all (onsite + remote)
        num_jobs_scanned = 0

        #for cur_url_to_scrape, num_pgs_to_scrape in comprehensive_combo:
        #for cur_url_to_scrape, num_pgs_to_scrape in backup_combo:
        # all_combos includes both comprehensive and backup
        combos_to_use = comprehensive_combo_30D #auto_apply_remote_combo #comprehensive_combo #all_combos  # backup_combo - all last 7d vs comprehensive_combo - mix of res from last 7d
        for cur_url_to_scrape, num_pgs_to_scrape in combos_to_use:

            scrape_job_listings_multiproc(
                                              process_cnt = 6,
                                              num_pgs_to_scan = num_pgs_to_scrape,
                                              url_to_scrape = cur_url_to_scrape,
                                              _start_page = 0
                                             )
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        num_jobs_scanned = 0.0000001 # avoid div by zero

    end_time = time()
    elapsed_time = end_time - start_time
    if num_jobs_scanned == 0:
        num_jobs_scanned = 0.0001
    print(f"Elapsed time: {elapsed_time:.2f} seconds for scanning #: {num_jobs_scanned} jobs using scrape_job_listings_multiproc")
    print(f"multiproc avg: {elapsed_time/num_jobs_scanned:.2f} seconds per job scanned")
    print(f"multiproc avg: {num_jobs_scanned/elapsed_time:.2f} jobs scanned per second")

# est .9s * 17000 = 15300s / 60s = 255min == 4hr 15min


# ToDo containerize script?  // pyenv

## extrapolating from mini-test
##  est 12.5hrs to scan 56k on 8proc
##  est 10.5hrs to scan 56k on 16proc
## wild guess 11.5hrs to scan 56k on 12proc
'''
mini test results using ryzen 4900h 8 proc/thread, 32pages to scan
multiproc ~2x faster than multithread
    multiproc avg: 0.77 seconds per job scanned
    multiproc avg: 1.31 jobs scanned per second

    multithread avg: 1.55 seconds per job scanned
    multithread avg: 0.65 jobs scanned per second

#--------------------------------------------------------------------
2nd mini test to find optimal process pool size
8-16 on 8c ryzen 4900h seems best,
32proc seemingly slower than 16proc, and 4proc 2x 8proc

Elapsed time (4proc): 980.91 seconds for scanning #: 640 jobs
(4proc) avg: 1.53 seconds per job scanned
(4proc) avg: 0.65 jobs scanned per second

Elapsed time (8proc): 526.43 seconds for scanning #: 640 jobs
(8proc) avg: 0.82 seconds per job scanned
(8proc) avg: 1.22 jobs scanned per second

Elapsed time (16proc): 408.75 seconds for scanning #: 640 jobs
(16proc) avg: 0.64 seconds per job scanned
(16proc) avg: 1.57 jobs scanned per second

Elapsed time (32proc): 414.36 seconds for scanning #: 624 jobs
(32proc) avg: 0.66 seconds per job scanned
(32proc) avg: 1.51 jobs scanned per second
'''

'''
start_time = time()
try:
    num_jobs_scanned4 = scrape_job_listings_multithread(
                                          threadpool_cnt = 4,
                                          num_pgs_to_scan = 32,
                                          _start_page = 0
                                         )
except Exception as e:
    print(e)
    print(traceback.format_exc())
    num_jobs_scanned4 = 0

end_time = time()
elapsed_time4 = end_time - start_time

#--------------------------------------------------------------------

start_time = time()
try:
    num_jobs_scanned8 = scrape_job_listings_multithread(
                                          threadpool_cnt = 8,
                                          num_pgs_to_scan = 32,
                                          _start_page = 0
                                         )
except Exception as e:
    print(e)
    print(traceback.format_exc())
    num_jobs_scanned8 = 0

end_time = time()
elapsed_time8 = end_time - start_time


#--------------------------------------------------------------------

start_time = time()
try:
    num_jobs_scanned16 = scrape_job_listings_multithread(
                                          threadpool_cnt = 16,
                                          num_pgs_to_scan = 32,
                                          _start_page = 0
                                         )
except Exception as e:
    print(e)
    print(traceback.format_exc())
    num_jobs_scanned16 = 0

end_time = time()
elapsed_time16 = end_time - start_time

#--------------------------------------------------------------------


start_time = time()
try:
    num_jobs_scanned32 = scrape_job_listings_multithread(
                                          threadpool_cnt = 32,
                                          num_pgs_to_scan = 32,
                                          _start_page = 0
                                         )
except Exception as e:
    print(e)
    print(traceback.format_exc())
    num_jobs_scanned32 = 0

end_time = time()
elapsed_time32 = end_time - start_time

#--------------------------------------------------------------------


print(f"Elapsed time (4proc): {elapsed_time4:.2f} seconds for scanning #: {num_jobs_scanned4} jobs")
print(f"(4proc) avg: {elapsed_time4/num_jobs_scanned4:.2f} seconds per job scanned")
print(f"(4proc) avg: {num_jobs_scanned4/elapsed_time4:.2f} jobs scanned per second\n")

print(f"Elapsed time (8proc): {elapsed_time8:.2f} seconds for scanning #: {num_jobs_scanned8} jobs")
print(f"(8proc) avg: {elapsed_time8/num_jobs_scanned8:.2f} seconds per job scanned")
print(f"(8proc) avg: {num_jobs_scanned8/elapsed_time8:.2f} jobs scanned per second\n")

print(f"Elapsed time (16proc): {elapsed_time16:.2f} seconds for scanning #: {num_jobs_scanned16} jobs")
print(f"(16proc) avg: {elapsed_time16/num_jobs_scanned16:.2f} seconds per job scanned")
print(f"(16proc) avg: {num_jobs_scanned16/elapsed_time16:.2f} jobs scanned per second\n")

print(f"Elapsed time (32proc): {elapsed_time32:.2f} seconds for scanning #: {num_jobs_scanned32} jobs")
print(f"(32proc) avg: {elapsed_time32/num_jobs_scanned32:.2f} seconds per job scanned")
print(f"(32proc) avg: {num_jobs_scanned32/elapsed_time32:.2f} jobs scanned per second\n")

'''
#--------------------------------------------------------------------

# ToDo run profiler on this and find the memory leak
#  the job desc pull version of this runs a lot smoother ram wise using the same chunk size
