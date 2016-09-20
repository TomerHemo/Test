import webapp2

class TransferHandler(webapp2.RequestHandler):

    def get(self):
        from pprint import pprint

    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials

    # Authentication is provided by the 'gcloud' tool when running locally
    # and by built-in service accounts when running on GAE, GCE, or GKE.
    # See https://developers.google.com/identity/protocols/application-default-credentials for more information.
    credentials = GoogleCredentials.get_application_default()

    # Construct the bigquery service object (version v2) for interacting
    # with the API. You can browse other available API services and versions at
    # https://developers.google.com/api-client-library/python/apis/
    service = discovery.build('bigquery', 'v2', credentials=credentials)


    # TODO: Change placeholders below to appropriate parameter values for the 'insert' method:

    # * Project ID of the project that will be billed for the job
    projectId = 'omega-strand-134923'

    job_body = {
      # TODO: Add desired entries of the 'job_body' dict
      "configuration": {
        "query": {
        "query": "SELECT Campaign_ID FROM `omega-strand-134923.ddm_dt_v2_us.match_table_placements_1684_20160918` LIMIT 5",
        "allowLargeResults": true, 
        "useLegacySql": false,
        #"destinationTable": {
      #"projectId": "omega-strand-134923",
      #"datasetId": "ddm_dt_v2_us",
      #"tableId": "CrossDevice_CID_UserID"
    #},
    #"createDisposition": "CREATE_IF_NEEDED",
    #"writeDisposition": "WRITE_TRUNCATE",
   }
    }
    }

    request = service.jobs().insert(projectId=projectId, body=job_body)
    response = request.execute()

    # TODO: Change code below to process the 'response' dict:
    pprint(response)


app = webapp2.WSGIApplication([
    ('/transfer', TransferHandler),
], debug=True)



