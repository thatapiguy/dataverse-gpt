import streamlit as st
import requests
import json
import os 
import uuid
import pandas as pd

from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain


def get_access_token(tenant_id, client_id, client_secret, resource):
    oauth2_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource
    }

    response = requests.post(oauth2_url, data=payload)
    if response.status_code == 200:
        response_json = response.json()
        access_token = response_json['access_token']
        #st.write(access_token)
        return access_token
    else:
        return f"Error : {response.text}"
    
# Create a function to generate data using OpenAI GPT-3
def generate_sample_data( table_entitysetname,data_format,number_of_rows):
    data_template = PromptTemplate(
        input_variables = ['format','table_entitysetname','rows'], 
        template="Generate sample data containing {rows} records for {table_entitysetname} table as a JSON array. Get inspiration from the data from {format} and generate the result in the same format. The output JSON should exclude the @odata.etag property and any property name (like accountid) that contains the text 'id' . Replace single quotes with double quotes in the result."
    )
    llm = OpenAI(temperature=0.7,model="text-davinci-003",max_tokens=500) 
    data_chain = LLMChain(llm=llm, prompt=data_template,verbose=True)
    generated_data = data_chain.run(format=data_format,table_entitysetname=table_entitysetname,rows=number_of_rows)
    return generated_data


def get_table_data_format(org_url,table_name,table_entitysetname,access_token):
    #api_endpoint = f"{org_url}/api/data/v9.2/{table_entitysetname}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    saved_query_name = "All "+table_entitysetname
    saved_query_url = f"{org_url}/api/data/v9.2/savedqueries?$select=savedqueryid&$filter=returnedtypecode%20eq%20'{table_name}'"
    #st.write(saved_query_url)
    query_response = requests.get(url=saved_query_url, headers=headers)
    query_data = query_response.json()
    #st.write(query_data)
    if "@odata.nextLink" not in query_data and len(query_data["value"]) > 0:
        saved_query_id = query_data["value"][0]["savedqueryid"]
        
        # Fetch and display the results of the saved query
        results_url = f"{org_url}/api/data/v9.0/{table_entitysetname}?savedQuery={saved_query_id}"
        #st.write(results_url)
        results_response = requests.get(url=results_url, headers=headers)
        entity_rows = results_response.json()
        #st.write(json.dumps(entity_rows, indent=4))
        entity_format = str(entity_rows["value"][:2])
        
    else:
        print("Error: Saved query not found.")

    #response = requests.get(api_endpoint, headers=headers)
    #st.write(response.text)
    #entity_format = response.json()
    return entity_format
    
def create_batch_request(headers, org_url,resource, data_rows):  
    batch_id = uuid.uuid4()
    batch_headers = {
        "Content-Type": f"multipart/mixed; boundary=batch_{batch_id}"
    }
    batch_headers.update(headers)

    # Create batch request body with changeset
    changeset_id = uuid.uuid4()
    request_body = f"--batch_{batch_id}\n"
    request_body += f"Content-Type: multipart/mixed; boundary=changeset_{changeset_id}\n\n"

    for data_row in data_rows:
        content_id = uuid.uuid4()
        request_body += f"--changeset_{changeset_id}\n"
        request_body += "Content-Type: application/http\n"
        request_body += f"Content-Transfer-Encoding: binary\n"
        request_body += f"Content-ID: {content_id}\n\n"
        request_body += f"POST {org_url}/api/data/v9.2/{resource} HTTP/1.1\n"
        request_body += f"Content-Type: application/json\n\n"
        request_body += json.dumps(data_row, ensure_ascii=False) + "\n\n"

    request_body += f"--changeset_{changeset_id}--\n"
    request_body += f"--batch_{batch_id}--"

    return requests.post(f"{org_url}/api/data/v9.2/$batch", headers=batch_headers, data=request_body.encode('utf-8'))




# Utility function to get Session State


def main():
    st.title('Dataverse Sample Data Generator')

    openai_api_key_user = st.text_input("OpenAI API Key",type="password")
    tenant_id = st.text_input("Tenant ID", value="", type="password")
    client_id = st.text_input("Client ID", value="", type="password")
    client_secret = st.text_input("Client Secret", value="", type="password")
    org_url = st.text_input("Organization URL", value="")
    resource = org_url
    table_entitysetname = st.text_input("Table's Plural Name/Entitysetname", value="",placeholder="e.g accounts, activities")

    number_of_rows = st.text_input("Number of Rows", value="")

    os.environ['OPENAI_API_KEY'] = openai_api_key_user
    if table_entitysetname.endswith("ies"):
        table_name= table_entitysetname[:-3] + "y"
    elif table_entitysetname.endswith("s"):
        table_name=table_entitysetname[:-1]
    

    # Initialize the button states if not already set
   

    if st.button("Generate Rows"):
        if not tenant_id or not client_id or not client_secret or not resource or not org_url:
            st.error("Please provide all required values.")
        else:
            access_token = get_access_token(tenant_id, client_id, client_secret, resource)
            #columns=get_columns(org_url,table_name,access_token)
            data_format = get_table_data_format(org_url,table_name,table_entitysetname,access_token)
            generated_data = generate_sample_data(table_entitysetname,data_format,number_of_rows)
            #st.write(f"Generated data: {generated_data}")

            data=json.loads(generated_data)
            tabular_data=pd.DataFrame(data)
            st.write(tabular_data)

            headers = {
                "Authorization": f"Bearer {access_token}",
            }
            resource= table_entitysetname
            response = create_batch_request(headers,org_url,resource, data)
            if response.status_code == 200:
                st.write("Data Added successfully.")
            else:
                print(f"Batch request failed. Status code: {response.status_code}")
                print("Response content:")
                print(response.content)
       

if __name__ == "__main__":
    main()      