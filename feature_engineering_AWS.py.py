import io
import logging
import re
import sys

import numpy as np
import pandas as pd

import boto3
from awsglue.utils import getResolvedOptions
args = getResolvedOptions(sys.argv, ['s3swampbucket','s3lakebucket'])


boto3.setup_default_session(profile_name="BFAS-Sandbox-Standard")
BUCKET = args['s3swampbucket']
#PREFIX = 'People/External/Pet_Sales_Craigslist/year=2020/month=01/'
#FINAL_FILE = 'People/External/Pet_Sales/year=2020/month=01/2020_01_final_cl_data.csv'
PREFIX = 'People/External/Pet_Sales_Craigslist/year=2019/month=12/'
FINAL_FILE = 'People/External/Pet_Sales/year=2019/month=12/2019_12_final_cl_data.csv'

# Open client to s3
s3 = boto3.client('s3')

# Load breed/species search terms and display name files 
logging.info("Loading breed/species files.")
dog_breeds_key='People/External/Pet_Sales_Craigslist/breed_list.csv'
s3_obj = s3.get_object(Bucket=BUCKET, Key=dog_breeds_key)
dog_breeds = pd.read_csv(io.BytesIO(s3_obj['Body'].read()))

cat_breeds_key='People/External/Pet_Sales_Craigslist/cat_breeds.csv'
s3_obj = s3.get_object(Bucket=BUCKET, Key=cat_breeds_key)
cat_breeds = pd.read_csv(io.BytesIO(s3_obj['Body'].read()))

other_species_key='People/External/Pet_Sales_Craigslist/other_species.csv'
s3_obj = s3.get_object(Bucket=BUCKET, Key=other_species_key)
other_species = pd.read_csv(io.BytesIO(s3_obj['Body'].read()), encoding = 'unicode_escape')

FilesNotFound = True
files=[]
cl_data = pd.DataFrame()
for obj in s3.list_objects(Bucket=BUCKET, Prefix=PREFIX)['Contents']:
    f = obj['Key'][56:]
    if f !="":
        if "images" not in f:
            files.append(f)
        FilesNotFound = False
if FilesNotFound:
    logging.warn("No file in {0}/{1}".format(BUCKET, PREFIX))
for file in files:
        file_src=PREFIX+str(file)
        logging.info("Downloading: "+file_src)
        s3_obj = s3.get_object(Bucket=BUCKET, Key=str(file_src))
        df = pd.read_csv(io.BytesIO(s3_obj['Body'].read()), sep='\\t', 
            names=['violation_category','craigslist_section', 'state_city', 
                    'limit', 'ad_url', 'date_posted', 'ad_id', 
                    'parent_ad_id', 'ad_title', 'ad_text', 'latitude', 
                    'longitude', 'unknown_num3', 'title_flags', 
                    'ad_text_flags', 'title_non_flags', 'ad_text_non_flags'],
                    engine='python')
        cl_data = cl_data.append(df)
del df

# Create joined text field with title & ad body
all_ad_text = cl_data['ad_title'] + cl_data['ad_text']
all_ad_text = all_ad_text.str.lower()
logging.info("Total number of ads found: " + str(len(all_ad_text)))

def str_series_contains_a_substr_in_list(strings, substr_list):
    '''
    Arguments:
    -strings (pandas Series of str)
    -substr_list (list of str)
    
    Returns:
    List of int (1 or 0). The int reflects whether 
        at least one of the str in the substr_list is found in the 
        Series of strings.
    '''
    test = '|'.join(map(re.escape, substr_list))
    return strings.str.contains(test, na=False, regex=True).astype(int).tolist()

#### Search for dogs & breeds
logging.info("Adding display names for breeds/species.")

# Search dog listings
checks = ['dog ', 'puppy', 'doggy', 'dogs', 'doggies', 'pup', 'pooch', 'mutt']
cl_data['check_dogs'] = str_series_contains_a_substr_in_list(all_ad_text, checks)

# Search for all dog breeds in each all_ad_text
breed_abbr = dog_breeds['breed_search_term'].str.lower()
cl_breeds = []
for item in all_ad_text:
    breed = 'NA'
    for term in breed_abbr:
        if term in item:
            breed = term.title()
    cl_breeds.append(breed)
cl_data['breed_search_term_new'] = cl_breeds
cl_data = cl_data.merge(dog_breeds, left_on='breed_search_term_new',
                                right_on='breed_search_term',how='left')
cl_data = cl_data.drop('breed_search_term_new', axis=1)
del dog_breeds

#### Search for cats
# Important note on cats: This will also catch ads that say 
#  "Good with cats" or similar language.
checks = ['cat ', 'kitten', 'kitty','cats', 'kitties']
cl_data['check_cats'] = str_series_contains_a_substr_in_list(all_ad_text, checks)

# Search for cat breeds in each all_ad_text
cat_breed_abbr = cat_breeds['cat_search_term'].str.lower()
cl_cat_breeds = []
for item in all_ad_text:
    cat_breed = 'NA'
    for term in cat_breed_abbr:
        if term in item:
            cat_breed = term.title()
    cl_cat_breeds.append(cat_breed)
cl_data['cat_breed_search_term_new'] = cl_cat_breeds
cl_data = cl_data.merge(cat_breeds,left_on='cat_breed_search_term_new',
                                    right_on='cat_search_term',how='left')
cl_data = cl_data.drop('cat_breed_search_term_new', axis=1)
del cat_breeds

# Search for other species
species_lower = other_species['other_search_term'].str.lower()
cl_species = []
for item in all_ad_text:
    species = 'NA'
    for term in species_lower:
        if term in item:
            species = term
    cl_species.append(species)
cl_data['species_search_term_new'] = cl_species
cl_data = cl_data.merge(other_species, left_on='species_search_term_new',
                                    right_on='other_search_term',how='left')
cl_data = cl_data.drop('species_search_term_new', axis=1)
del other_species

#### Search for relevant substrings & add columns
logging.info("Adding flag columns based on string searches.")

check = ['akc']
cl_data['check_akc'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['deposit']    
cl_data['check_deposit'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['rehoming fee', 'rehomeing fee', 'rehousing fee']    
cl_data['check_rehoming_fee'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['for sale']       
cl_data['check_for_sale'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['registered', 'registration']       
cl_data['check_registered'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['teacup', 'tea cup']  
cl_data['check_teacup'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['kennel']       
cl_data['check_kennel'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['stud', '$tud']       
cl_data['check_stud'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['micro']      
cl_data['check_micro'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['breeder']       
cl_data['check_breeder'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['hypoallergenic', 'hypo-allergenic', 'hyperallergenic', 'hyper-allergenic',
         'hypo allergenic', 'hyper allergenic']     
cl_data['check_hypoallergenic'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['pedigree', 'pedigreed']        
cl_data['check_pedigree'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['to ensure', 'to insure']      
cl_data['check_to_ensure'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['sire']
cl_data['check_sire'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['dam']
cl_data['check_dam'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['tails docked', 'docked tails', 'tail docked', 'docked tail',
         'tail is docked', 'tails are docked']       
cl_data['check_tail_docked'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['not cheap']
cl_data['check_not_cheap'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['ears cropped', 'cropped ears', 'ears are cropped']       
cl_data['check_ears_cropped'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['in tact', 'intact', 'in-tact', 'unneutered', 'un-neutered', 'not neutered',
         "isn't spayed", "hasn't been spayed", "haven't been spayed", 'unspayed',
         'un-spayed', 'not spayed', "isn't neutered", "hasn't been neutered",
         "haven't been neutered", "not fixed", "isn't fixed", "hasn't been fixed",
         "aren't fixed"]
cl_data['check_in_tact'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['litter']
cl_data['check_litter'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['www', '.com']
cl_data['check_url'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['designer']
cl_data['check_designer'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['purebreed', 'purebread', 'prebred', 'pure breed', 'pure bred',
         'pure bread', 'pure-breed', 'pure-bread', 'pure-bred']       
cl_data['check_purebred'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['pups']
cl_data['check_pups'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['no lowball', 'no low ball', 'no low-ball']  
cl_data['check_no_lowballers'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['not free']    
cl_data['check_not_free'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['payment plan']
cl_data['check_payment_plan'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['cash only', 'ca$h only']    
cl_data['check_cash_only'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['papers']
cl_data['check_papers'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['previous litter', 'last litter']    
cl_data['check_previous_last_litter'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['serious inquiries', 'serious enquiries']    
cl_data['check_serious_inquiries'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['champion bloodline']
cl_data['check_champion_bloodline'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['health guarantee']
cl_data['check_health_guarantee'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['not for sale']
cl_data['check_not_for_sale'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['exotic']
cl_data['check_exotic'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['iccf']
cl_data['check_iccf'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['do your research']
cl_data['check_do_your_research'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['proven']
cl_data['check_proven'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['facebook', 'instagram']
cl_data['check_social'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['financing']
cl_data['check_financing'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['in heat', 'in-heat', 'inheat']
cl_data['check_in_heat'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['donation']
cl_data['check_donation'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['relocation fee']
cl_data['check_relocation_fee'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['$$$']
cl_data['check_$$$'] = str_series_contains_a_substr_in_list(all_ad_text, check)

check = ['f1b']
cl_data['check_f1b'] = str_series_contains_a_substr_in_list(all_ad_text, check)

#### Looking for people warning of fraud (we want these ads out there).

# Search 'scam' & 'fraud' assign to new column
check = ['scam', 'fraud']    
cl_data['check_fraud_alert'] = str_series_contains_a_substr_in_list(all_ad_text, check)

#### Search for dollar sign & subsequent value to capture standard fee listings
logging.info("Adding flags based on fee search.")

# Find simple amounts with '$X' format
amounts = []
for item in all_ad_text:
    amount = re.findall('\$[0-9]+', item)
    amounts.append(amount)
new_amounts = []
for lst in amounts:
    flt_lst = []
    for item in lst:
        item = str(item)
        item = item.strip('$')
        item = float(item)
        flt_lst.append(item)
    new_amounts.append(flt_lst)
amts = []
for lst in new_amounts:
    if lst == []:
        val = 0
    else: val = max(lst)
    amts.append(val)
cl_data['regex_dollar_sign_amt'] = amts

# Find amounts in the format "Xk" or "XK"
amounts_k_thousands = []
for item in all_ad_text:
    amount = re.findall('[0-9]+[Kk]',item)
    amounts_k_thousands.append(amount)

new_amounts_k_thousands = []    
for lst in amounts_k_thousands:
    flt_lst = []
    for item in lst:
        item = str(item)
        item = item.strip('k')
        item = item.strip('K')
        item = float(item)*1000
        flt_lst.append(item)
    new_amounts_k_thousands.append(flt_lst)
amts_k_thousands = []
for lst in new_amounts_k_thousands:
    if lst == []:
        val = 0
    else: val = max(lst)
    amts_k_thousands.append(val)
cl_data['regex_k_thousands_amt'] = amts_k_thousands

# Find amounts before the word "Thousand" and "thousand"
amounts_thousands = []
for item in all_ad_text:
    amount = re.findall('[0-9]+\s[Tt]housand',item)
    amounts_thousands.append(amount)
new_amounts_thousands = []
for lst in amounts_thousands:
    flt_lst = []
    for item in lst:
        item = str(item).strip(' Thousand').strip(' thousand')
        item = float(item)*1000
        flt_lst.append(item)
    new_amounts_thousands.append(flt_lst)
amts_thousands = []
for lst in new_amounts_thousands:
    if lst == []:
        val = 0
    else: val = max(lst)
    amts_thousands.append(val)
cl_data['regex_thousands_amt'] = amts_thousands

# Find amounts before the word "Hundred" and "hundred"
amounts_hundred = []
for item in all_ad_text:
    amount = re.findall('[0-9]+\s[Hh]undred',item)
    amounts_hundred.append(amount)
new_amounts_hundred = []
for lst in amounts_hundred:
    flt_lst = []
    for item in lst:
        item = str(item).strip(' Hundred').strip(' hundred')
        item = float(item)*100
        flt_lst.append(item)
    new_amounts_hundred.append(flt_lst)
amts_hundred = []
for lst in new_amounts_hundred:
    if lst == []:
        val = 0
    else: val = max(lst)
    amts_hundred.append(val)
cl_data['regex_hundreds_amt'] = amts_hundred
cl_data['fee_amt'] = cl_data[['regex_dollar_sign_amt','regex_thousands_amt','regex_k_thousands_amt',
                              'regex_hundreds_amt']].max(axis=1)
cl_data['fee_amt'] = cl_data['fee_amt'].replace(0,np.nan)

# Search for phone numbers
logging.info("Search for phone numbers.")
phone_numbers = []
for item in all_ad_text:
    number = str(re.findall('(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',item))
    phone_numbers.append(number)
cl_data['phone_num'] = phone_numbers

# Capture emails in format: name(at)mail.com
logging.info("Search for email addresses.")
emails1 = []
for item in all_ad_text:
    email = re.findall('\S*\s?\(+at\)\s?\S*',item)
    emails1.append(email)
cl_data['emails1'] = emails1
del emails1

# Capture emails in format: leejamzy247 @ gmail . com   
emails2 = []
for item in all_ad_text:
    email = re.findall('\S*\s?@\s?\S*\s*.\s*com',item)
    emails2.append(email)
cl_data['emails2'] = emails2
del emails2

# Combine 2 email searches and keep only the first search match.
cl_data['emails'] = cl_data['emails1'] + cl_data['emails2']
cl_data['emails'] = cl_data['emails'].str[0]
cl_data.drop(['emails1','emails2'], axis=1, inplace=True)

# Check for websites
logging.info("Search for websites.")
websites = []
for item in all_ad_text:
    website = str(re.findall('\s[^\s]+.com\s', item))
    websites.append(website)
cl_data['websites'] = websites
del websites

## Check for similar/duplicate ads across communities
# After investigation, it looks like the 'parent_ad_id' column contains 
#  indicators of duplicate (or very similar posts). This is our best bet for identifying mass-posters.  
#  However, some of these are rescues or shelters posting animals for adoption. 
#  We'll add a field for teasing these out from adoption agencies vs. public posters. 
#  This info, in combination with the 'parent_ad_id' column can be used to visualize dups. 
#  Note: There seem to be a LOT of parrots in these dup posts - potential callout.

#Search for indicators of Shelters/Rescue posts to differentiate these dup ads from public posts.
check = ['rescue', 'county', 'shelter', 'humane society', 'humane association', 
         '501c3', '501(c)', 'nonprofit', 'non profit', 'non-profit']
cl_data['check_shelter_rescue'] = str_series_contains_a_substr_in_list(all_ad_text, check)

# # Final field for potential violation
# Bring together all indicators of violation and create one master field for 'Likely Violation'.
# Check for simple phrases that point to probable violation.
logging.info("Adding field for potential violations.")
violation_indicators = ['check_deposit','check_for_sale', 'check_stud', 'check_breeder', 'check_sire', 'check_dam',
                       'check_tail_docked', 'check_not_cheap', 'check_ears_cropped', 'check_in_tact', 'check_no_lowballers', 
                       'check_payment_plan', 'check_cash_only', 'check_previous_last_litter', 'check_champion_bloodline', 
                       'check_health_guarantee', 'check_not_for_sale', 'check_not_free']
cl_data['violation_sum'] = cl_data[violation_indicators].sum(axis=1)

# Check for ads that mention rehoming fee but don't list an obvious fee. Output is boolean.
cl_data['rehome_no_fee'] = cl_data['fee_amt'].isnull() & (cl_data['check_rehoming_fee'] > 0)

# Check for ads with parent id values AND check_shelter_rescue not equal to 1.
cl_data['duplicates'] = cl_data['parent_ad_id'].notnull() & (cl_data['check_shelter_rescue'] == 0)

# Bring the 3 above checks together and add in any with fees > $250 for final check.

cl_data['likely_violation'] = ((cl_data['violation_sum'] > 0) | 
                                (cl_data['rehome_no_fee'] == True) | 
                                (cl_data['duplicates'] == True) | 
                                (cl_data['fee_amt'] > 250))


# Pull out city and state of posting
city = cl_data['ad_url'].str.split('//',n=1,expand=True)
city2 = city[1].str.split('.',n=1,expand=True)
cl_data['City'] = city2[0]
cl_data['State'] = cl_data['state_city'].str[:2]

cl_data.info()
 
# Remove unneccesary columns:
cl_data.drop(['craigslist_section','limit','ad_url','regex_dollar_sign_amt','regex_k_thousands_amt',
                           'regex_thousands_amt','regex_hundreds_amt',
                           'violation_sum','rehome_no_fee','duplicates'],axis=1, inplace=True)

logging.info("Saving: "+ FINAL_FILE + " to s3 Bucket: " + BUCKET)
csv_buffer = io.StringIO()
cl_data.to_csv(csv_buffer)
csv_buffer.seek(0)
s3.put_object(Bucket='bfas-sandbox-s3-lake', Key=FINAL_FILE,
                Body=csv_buffer.getvalue(), ServerSideEncryption="AES256")

logging.info("Done!")
