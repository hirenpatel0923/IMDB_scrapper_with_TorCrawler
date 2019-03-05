from watson_developer_cloud import PersonalityInsightsV3
import pandas as pd
import time

data = pd.read_csv('filtered_data.csv', encoding='utf-8')
small_data = data.drop(labels=['movieId','username','user_link','ratings'], axis=1)
print(small_data.count())

dic = {
    #'0':['00c396f8-50dc-4dfe-aabf-41578d622595','Pxk4m5ZZp5qr'],
    #'1':['3676eb88-3271-4dc4-970e-6d4745804c71','P6xXorVWzOaD'],
    '2':['b5614d95-4d2c-474d-a55d-35f59b92afa8','f15VK8WWfteZ'],
    '3':['fbd53a54-2b25-44b2-883c-db865b46fc3e','FtkbczWtkpa7'],
    '4':['d5507058-5070-4125-868f-be73e361005e','8J1d6zcgauu7']
}

base = 1600
diff = 800

for key in dic.keys():
    auth = dic[key]
    username = auth[0]
    password = auth[1]

    start = base + (diff * int(key))
    end = start + diff

    temp = small_data.loc[start:end,:]

    PI = PersonalityInsightsV3(version='2017-10-13', username=username,
        password=password)

    time_start = time.clock()
    
    for index, review in temp['review'].iteritems():
        
        #Get the words in the review
        count = len(review.split()) 
        
        #Check if it's greater than 100
        if count >= 120:
        
            #Enough words to feed into PI
            #Pass on the review to PI
            json_output = PI.profile(review,content_type='text/plain')

            #Iterate over the result (having heirarchy)
            for i in json_output['personality']:
                for j in i['children']:
                    #Append the attributes to the data
                    data.set_value(index, j['name'], j['percentile']) 
                            
            for i in json_output['needs']:
                #Append the attributes to the data
                data.set_value(index, i['name'], i['percentile']) 
                
            for i in json_output['values']:
                #Append the attributes to the data
                data.set_value(index, i['name'], i['percentile']) 
    #-------------------------------------------------------------------------

    #Get the current time again and subract from 
    #previous to measure the time taken        
    time_end = time.clock() - time_start

    #Print the time taken
    print(time_end)
                    
    #Save the enriched data to another CSV File
    data.to_csv('test'+str(key)+'.csv')

