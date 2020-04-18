import sys
import numpy as np
import pandas as pd
import re
import boto3
from subprocess import call
#call('rm -rf /tmp/*', shell=True)
s3=boto3.resource('s3')
bucket = s3.Bucket(name="bfas-developer-s3-swamp")
cat_breeds='People/External/Pet_Sales_Craigslist/cat_breeds.csv'
breeds='People/External/Pet_Sales_Craigslist/breed_list.csv'
other='People/External/Pet_Sales_Craigslist/other_species.csv'
s3.Bucket('bfas-developer-s3-swamp').download_file(cat_breeds,'/tmp/'+cat_breeds[37:51])
print(cat_breeds[37:]+' downloaded')
s3.Bucket('bfas-developer-s3-swamp').download_file(breeds,'/tmp/'+breeds[37:51])
print(breeds[37:51]+' downloaded')
s3.Bucket('bfas-developer-s3-swamp').download_file(other,'/tmp/'+other[37:54])
print(other[37:54]+' downloaded')
prefix = 'People/External/Pet_Sales_Craigslist/year=2020/month=01/'
final_file = 'People/External/Pet_Sales/year=2020/month=01/2020_01_final_cl_data.csv'
FilesNotFound = True
files=[]
cl_data=pd.DataFrame()
for obj in bucket.objects.filter(Prefix=prefix):
    f=obj.key[56:]
    print(f)
    if f !="":
        if "images" not in f:
            files.append(f)
        FilesNotFound = False
print(files)
if FilesNotFound:
    print("ALERT", "No file in {0}/{1}".format(bucket, prefix))
for file in files:
        file_src=prefix+str(file)
        tmp='/tmp/'+str(file)
        print("Downloading: "+file)
        s3.Bucket('bfas-developer-s3-swamp').download_file(file_src,tmp)
        df = pd.read_csv(tmp, sep = '\t', names=['violation_category','craigslist_section', 'state_city', 
                                                                        'limit', 'ad_url', 'date_posted', 'ad_id', 
                                                                        'parent_ad_id', 'ad_title', 'ad_text', 'latitude', 
                                                                        'longitude', 'unknown_num3', 'title_flags', 
                                                                        'ad_text_flags', 'title_non_flags', 'ad_text_non_flags'])
        cl_data=cl_data.append(df)
#cl_data.to_csv('/tmp/cl_data.csv', sep='\t', encoding='utf-8')
 ## Search for cats
    #Important note on cats: This will also catch ads that say \Good with cats\ or similar language.
    # Search cat listings
# Create joined text field with title & ad body
all_ad_text = cl_data['ad_title'] + cl_data['ad_text']
#all_ad_text = all_ad_text.str.lower()
    ## Search for dogs & breeds
    # Search dog listings
search = ['dog ','puppy','doggy','dogs','doggies','pup','pooch','mutt']
search_values=[]
for items in all_ad_text:
    if any(s in items for s in search):
        value=1
    else:
        value=0
    search_values.append(value)
cl_data['check_dogs'] = search_values 
print("check dogs added to the dataframe")

  # Import breed list 
breeds = pd.read_csv('/tmp/breed_list.csv')
#breeds.head(5)
# Search for all breeds in each all_ad_text
breed_abbr = breeds['breed_search_term'].str.lower()
cl_breeds = []
for item in all_ad_text:
        breed = 'x'
        for term in breed_abbr:
            search = item.find(term)
            if search > -1:
                breed = term.title()
        if breed == 'x':
            breed = 'NA'
        cl_breeds.append(breed)
        
cl_data['breed_search_term_new'] = cl_breeds
cl_data = cl_data.merge(breeds,left_on='breed_search_term_new',right_on='breed_search_term',how='left')
cl_data = cl_data.drop('breed_search_term_new', axis=1)
print("breed search term added")


 # Import other species list
species_df = pd.read_csv('/tmp/other_species.csv', encoding = 'unicode_escape')
    # Search for other species
species_lower = species_df['other_search_term'].str.lower()
cl_species = []
    
for item in all_ad_text:
        species = 'x'
        for term in species_lower:
            search = item.find(term)
            if search > -1:
                species = term
        if species == 'x':
            species = 'NA'
        cl_species.append(species)
        
cl_data['species_search_term_new'] = cl_species
cl_data = cl_data.merge(species_df,left_on='species_search_term_new',right_on='other_search_term',how='left')
 
    ## Search for relevant substrings & add columns
    # Search 'AKC' & assign to new column
search_values = []

for item in all_ad_text:
    search = item.find('financing')
    if search == -1:
        value = 0
    else: value = 1
    search_values.append(value)
cl_data['check_financing'] = search_values

search_values = []
for item in all_ad_text:
    search = item.find('in heat')
    if search == -1:
        search = item.find('in-heat')
        if search == -1:
            search = item.find('inheat')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
    else: value = 1
    search_values.append(value)
cl_data['check_in_heat'] = search_values

search_values = []
for item in all_ad_text:
    search = item.find('donation')
    if search == -1:
        value = 0
    else: value = 1
    search_values.append(value)
cl_data['check_donation'] = search_values

# Search 'teacup', 'tea cup' & assign to new column
search_values = []
for item in all_ad_text:
    search = item.find('teacup')
    if search == -1:
        search = item.find('tea cup')
        if search == -1:
            value = 0
        else: value = 1
    else: value = 1
    search_values.append(value)
        
cl_data['check_teacup'] = search_values

search_values = []
for item in all_ad_text:
    search = item.find('relocation fee')
    if search == -1:
        value = 0
    else: value = 1
    search_values.append(value)
cl_data['check_relocation_fee'] = search_values

search_values = []
for item in all_ad_text:
    search = item.find('$$$')
    if search == -1:
        value = 0
    else: value = 1
    search_values.append(value)
cl_data['check_$$$'] = search_values
print("check_$$$ done")



#search AKC
search_values = []
for item in all_ad_text:
        search = item.find('akc')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)            
cl_data['check_akc'] = search_values
print("AKC done")
# Search F1B and assign to new column. F1B is commonly used when advertising Goldendoodles
search_values = []
for item in all_ad_text:
    search = item.find('f1b')
    if search == -1:
        value = 0
    else: value = 1
    search_values.append(value)
cl_data['check_f1b'] = search_values
print("F1B done")
# Search 'Deposit' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('deposit')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
            
    # Search 'rehoming fee', 'rehousing fee', 'rehomeing fee' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('rehoming fee')
        if search == -1:
            search = item.find('rehomeing fee')
            if search == -1:
                search = item.find('rehousing fee')
                if search == -1:
                    value = 0
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_rehoming_fee'] = search_values
print('check_rehoming_fee added')
    # Search 'for sale' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('for sale')
        if search == -1:
            value = 0
        else:
            value = 1
        search_values.append(value)
            
cl_data['check_for_sale'] = search_values
 
    # Search 'registered' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('registered')
        if search == -1:
            search == item.find('registration')
            if search == -1:
                value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_registered'] = search_values
# Search cat listings
search_values = []
for item in all_ad_text:
    search = item.find('cat ')
    if search == -1:
        search = item.find('kitten')
        if search == -1:
            search = item.find('kitty')
            if search == -1:
                search = item.find('cats')
                if search == -1:
                    search = item.find('kitties')
                    if search == -1:
                        value = 0
                    else: value = 1
                else: value = 1
            else: value = 1
        else: value = 1
    else: value = 1
    search_values.append(value)
       
cl_data['check_cats'] = search_values
 
# Import cat breeds list
cat_breeds = pd.read_csv('/tmp/cat_breeds.csv')
# Search for all breeds in each all_ad_text
cat_breed_abbr = cat_breeds['cat_search_term'].str.lower()
cl_cat_breeds = []

for item in all_ad_text:
    cat_breed = 'x'
    for term in cat_breed_abbr:
        search = item.find(term)
        if search > -1:
            cat_breed = term.title()
    if cat_breed == 'x':
        cat_breed = 'NA'
    cl_cat_breeds.append(cat_breed)
    
cl_data['cat_breed_search_term_new'] = cl_cat_breeds
 
cl_data = cl_data.merge(cat_breeds,left_on='cat_breed_search_term_new',right_on='cat_search_term',how='left')
cl_data = cl_data.drop('cat_breed_search_term_new', axis=1)
print('cat breeds done')

    # Search 'teacup', 'tea cup' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('teacup')
        if search == -1:
            search = item.find('tea cup')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
cl_data.head()  

    # Search 'kennel' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('kennel')
        if search == -1:
            value = 0
        else:
            value = 1
        search_values.append(value)
            
cl_data['check_kennel'] = search_values

#cl_data.head()
 
    # Search 'stud', '$stud' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('stud')
        if search == -1:
            search = item.find('$tud')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_stud'] = search_values
    # Search 'micro' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('micro')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
            
cl_data['check_micro'] = search_values
 
    # Search 'breeder' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('breeder')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
            
cl_data['check_breeder'] = search_values
 
    # Search 'hypoallergenic', 'hypo-allergenic', 'hyperallergenic', 'hyper-allergenic', 'hypo allergenic', 'hyper allergenic' 
    # & assign to new column
    
search_values = []
for item in all_ad_text:
        search = item.find('hypoallergenic')
        if search == -1:
            search = item.find('hypo-allergenic')
            if search == -1:
                search = item.find('hyperallergenic')
                if search == -1:
                    search = item.find('hyper-allergenic')
                    if search == -1:
                        search = item.find('hypo allergenic')
                        if search == -1:
                            search = item.find('hyper allergenic')
                            if search == -1:
                                value = 0
                            else: value = 1
                        else: value = 1
                    else: value = 1
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_hypoallergenic'] = search_values
 
    # Search 'pedigree', 'pedigreed' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('pedigree')
        if search == -1:
            search = item.find('pedigreed')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_pedigree'] = search_values
 
    # Search 'to ensure', 'to insure' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('to ensure')
        if search == -1:
            search = item.find('to insure')
            if search == -1:
                value = 0
            else: value = -1
        else: value = 1
        search_values.append(value)
            
cl_data['check_to_ensure'] = search_values
 
    # Search 'sire' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('sire')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
            
cl_data['check_sire'] = search_values
 
    # Search 'dam' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('dam')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
            
cl_data['check_dam'] = search_values
 
    # Search 'tails docked', 'docked tails', 'tail docked', 'docked tail', 'tail is docked', 'tails are docked'
    # & assign to new column
    
search_values = []
for item in all_ad_text:
        search = item.find('tails docked')
        if search == -1:
            search = item.find('docked tails')
            if search == -1:
                search = item.find('tail docked')
                if search == -1:
                    search = item.find('docked tail')
                    if search == -1:
                        search = item.find('tail is docked')
                        if search == -1:
                            search = item.find('tails are docked')
                            if search == -1:
                                value = 0
                            else: value = 1
                        else: value = 1
                    else: value = 1
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_tail_docked'] = search_values
 
    # Search 'not cheap' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('not cheap')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
            
cl_data['check_not_cheap'] = search_values
 # Search 'ears cropped', 'cropped ears', 'ears are cropped', & assign to new column
    
search_values = []
for item in all_ad_text:
        search = item.find('ears cropped')
        if search == -1:
            search = item.find('cropped ears')
            if search == -1:
                search = item.find('ears are cropped')
                if search == -1:
                    value = 0
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_ears_cropped'] = search_values
 
    # Search 'in tact' etc & assign to new column
    
search_values = []
for item in all_ad_text:
        search = item.find('in tact')
        if search == -1:
            search = item.find('intact')
            if search == -1:
                search = item.find('in-tact')
                if search == -1:
                    search = item.find('unneutered')
                    if search == -1:
                        search = item.find('un-neutered')
                        if search == -1:
                            search = item.find('not neutered')
                            if search == -1:
                                search = item.find('isn\'t spayed')
                                if search == -1:
                                    search = item.find('hasn\'t been spayed')
                                    if search == -1:
                                        search = item.find('haven\'t been spayed')
                                        if search == -1:
                                            search = item.find('unspayed')
                                            if search == -1:
                                                search = item.find('un-spayed')
                                                if search == -1:
                                                    search = item.find('not spayed')
                                                    if search == -1:
                                                        search = item.find('isn\'t neutered')
                                                        if search == -1:
                                                            search = item.find('hasn\'t been neutered')
                                                            if search == -1:
                                                                search = item.find('haven\'t been neutered')
                                                                if search == -1:
                                                                    search = item.find('not fixed')
                                                                    if search == -1:
                                                                        search = item.find('isn\'t fixed')
                                                                        if search == -1:
                                                                            search = item.find('hasn\'t been fixed')
                                                                            if search == -1:
                                                                                search = item.find('aren\'t fixed')
                                                                                if search == -1:
                                                                                    value = 0
                                                                                else: value = 1
                                                                            else: value = 1
                                                                        else: value = 1
                                                                    else: value = 1
                                                                else: value = 1
                                                            else: value = 1
                                                        else: value = 1
                                                    else: value = 1
                                                else: value = 1
                                            else: value = 1
                                        else: value = 1
                                    else: value = 1
                                else: value = 1
                            else: value = 1
                        else: value = 1
                    else: value = 1
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_in_tact'] = search_values
 
    # Search 'litter' & assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('litter')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
            
cl_data['check_litter'] = search_values
 
    # Search 'www.' & '.com' assign to new column
search_values = []
for item in all_ad_text:
        search = item.find('www')
        if search == -1:
            search = item.find('.com')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_url'] = search_values
search_values = []
for item in all_ad_text:
        search = item.find('designer')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_designer'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('purebreed')
        if search == -1:
            search = item.find('purebread')
            if search == -1:
                search = item.find('prebred')
                if search == -1:
                    search = item.find('pure breed')
                    if search == -1:
                        search = item.find('pure bred')
                        if search == -1:
                            search = item.find('pure bread')
                            if search == -1:
                                search = item.find('pure-breed')
                                if search == -1:
                                    search = item.find('pure-bread')
                                    if search == -1:
                                        search = item.find('pure-bred')
                                        if search == -1:
                                            value = 0
                                        else: value = 1
                                    else: value = 1
                                else: value =1
                            else: value = 1
                        else: value = 1
                    else: value = 1
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_purebred'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('pups')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_pups'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('no lowball')
        if search == -1:
            search = item.find('no low ball')
            if search == -1:
                search = item.find('no low-ball')
                if search == -1:
                    value = 0
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
        
cl_data['check_no_lowballers'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('not free')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_not_free'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('payment plan')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_payment_plan'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('cash only')
        if search == -1:
            search = item.find('ca$h only')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
        
cl_data['check_cash_only'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('papers')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_papers'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('previous litter')
        if search == -1:
            search = item.find('last litter')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
        
cl_data['check_previous_last_litter'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('serious inquiries')
        if search == -1:
            search = item.find('serious enquiries')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
        
cl_data['check_serious_inquiries'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('champion bloodline')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_champion_bloodline'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('health guarantee')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_health_guarantee'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('not for sale')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_not_for_sale'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('exotic')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_exotic'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('iccf')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_iccf'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('do your research')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_do_your_research'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('proven')
        if search == -1:
            value = 0
        else: value = 1
        search_values.append(value)
        
cl_data['check_proven'] = search_values
 
search_values = []
for item in all_ad_text:
        search = item.find('facebook')
        if search == -1:
            search = item.find('instagram')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
        
cl_data['check_social'] = search_values

search_values = []
for item in all_ad_text:
        search = item.find('scam')
        if search == -1:
            search = item.find('fraud')
            if search == -1:
                value = 0
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_fraud_alert'] = search_values

amounts = []
for item in all_ad_text:
        amount = re.findall('\\$[0-9]+', item)
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
 
    # Find amounts in the format \Xk\ or \XK\
    
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
 
    # Find amounts before the word \Thousand\ and \thousand\
    
amounts_thousands = []
for item in all_ad_text:
        amount = re.findall('[0-9]+\\s[Tt]housand',item)
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
   # Find amounts before the word \Hundred\ and \hundred\
    
amounts_hundred = []
for item in all_ad_text:
        amount = re.findall('[0-9]+\\s[Hh]undred',item)
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
phone_numbers = []
for item in all_ad_text:
        number = str(re.findall('(\\d{3}[-\\.\\s]??\\d{3}[-\\.\\s]??\\d{4}|\\(\\d{3}\\)\\s*\\d{3}[-\\.\\s]??\\d{4}|\\d{3}[-\\.\\s]??\\d{4})',item))
        phone_numbers.append(number)
print(len(phone_numbers))
cl_data['phone_num'] = phone_numbers
 
    # Capture emails in format: name(at)mail.com
emails1 = []
for item in all_ad_text:
        email = re.findall('\\S*\\s?\\(+at\\)\\s?\\S*',item)
        emails1.append(email)
cl_data['emails1'] = emails1
 
#cl_data['emails1'].value_counts()
 

# Capture emails in format: leejamzy247 @ gmail . com   
emails2 = []
for item in all_ad_text:
        email = re.findall('\\S*\\s?@\\s?\\S*\\s*.\\s*com',item)
        emails2.append(email)
cl_data['emails2'] = emails2

#cl_data['emails2'].value_counts()
#cl_data.head() 
#Combine 2 email searches and keep only the first search match
cl_data['emails'] = cl_data['emails1'] + cl_data['emails2']
cl_data['emails'] = cl_data['emails'].str[0]
cl_data = cl_data.drop(['emails1','emails2'],axis=1)
 
websites = []
for item in all_ad_text:
        website = str(re.findall('\\s[^\\s]+.com\\s', item))
        websites.append(website)
print(len(websites))
cl_data['websites'] = websites
print("webite email done") 
#cl_data['parent_ad_id'].value_counts().head()
##Search for indicators of Shelters/Rescue posts to differentiate these dup ads from public posts.
    
search_values = []
for item in all_ad_text:
        search = item.find('rescue')
        if search == -1:
            search = item.find('county')
            if search == -1:
                search = item.find('shelter')
                if search == -1:
                    search = item.find('humane society')
                    if search == -1:
                        search = item.find('humane association')
                        if search == -1:
                            search = item.find('501c3')
                            if search == -1:
                                search = item.find('501(c)')
                                if search == -1:
                                    search = item.find('nonprofit')
                                    if search == -1:
                                        search = item.find('non profit')
                                        if search == -1:
                                            search = item.find('non-profit')
                                            if search == -1:
                                                value = 0
                                            else: value = 1
                                        else: value = 1
                                    else: value = 1
                                else: value =1
                            else: value = 1
                        else: value = 1
                    else: value = 1
                else: value = 1
            else: value = 1
        else: value = 1
        search_values.append(value)
            
cl_data['check_shelter_rescue'] = search_values

# Search 'Deposit' & assign to new column
search_values = []
for item in all_ad_text:
    search = item.find('deposit')
    if search == -1:
        value = 0
    else: value = 1
    search_values.append(value)
       
cl_data['check_deposit'] = search_values
    
print(sum(search_values))
    # Check for simple phrases that point to probable violation.

violation_indicators = ['check_deposit','check_for_sale', 'check_stud', 'check_breeder', 'check_sire', 'check_dam',
                           'check_tail_docked', 'check_not_cheap', 'check_ears_cropped', 'check_in_tact', 'check_no_lowballers', 
                           'check_payment_plan', 'check_cash_only', 'check_previous_last_litter', 'check_champion_bloodline', 
                           'check_health_guarantee', 'check_not_for_sale', 'check_not_free']
cl_data['violation_sum'] = cl_data[violation_indicators].sum(axis=1)
                         
print(cl_data['violation_sum'].sum())
 
    # Check for ads that mention rehoming fee but don't list an obvious fee. Output is boolean.
cl_data['rehome_no_fee'] = cl_data['fee_amt'].isnull() & (cl_data['check_rehoming_fee'] > 0)
 
    # Check for ads with parent id values AND check_shelter_rescue not equal to 1.
cl_data['duplicates'] = cl_data['parent_ad_id'].notnull() & (cl_data['check_shelter_rescue'] == 0)
 
    # Bring the 3 above checks together and add in any with fees > $250 for final check.
    
cl_data['likely_violation'] = (cl_data['violation_sum'] > 0) | (cl_data['rehome_no_fee'] == True) | (cl_data['duplicates'] == True) | (cl_data['fee_amt'] > 250)

cl_data['likely_violation'].value_counts()

    ## Run cell below to export csv once complete.
 
#cl_data['unknown_num3'].value_counts()
 
cl_data.info()
 
    # Insert code to remove unneccesary columns:
cl_data = cl_data.drop(['craigslist_section','limit','ad_url','regex_dollar_sign_amt','regex_k_thousands_amt',
                           'regex_thousands_amt','regex_hundreds_amt','violation_sum','rehome_no_fee','duplicates'],axis=1)
cl_data.to_csv('/tmp/cl_data.csv', sep=',', encoding='utf-8')
tmp = '/tmp/cl_data.csv'
client = boto3.client('s3', 'us-west-2')
transfer = boto3.s3.transfer.S3Transfer(client=client)
transfer.upload_file(tmp,'bfas-developer-s3-lake',final_file, extra_args={'ServerSideEncryption':"AES256"})
#s = boto3.client('s3')
#s.put_object(Bucket = 'bfas-developer-s3-lake', Key=final_file, Body=tmp, ServerSideEncryption="AES256")
print('done')



