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
        "writeDisposition": "WRITE_TRUNCATE",
        "query": {
          "allowLargeResults": true,
          "destinationTable": {
            "datasetId": "ddm_dt_v2_us",
            "projectId": "omega-strand-134923",
            "tableId": "CrossDevice_CID_UserID"
          },
          "query": "select distinct CID, User_ID,CURRENT_TIMESTAMP() as LastUpdateDate , RegDate,FTDDate from (SELECT REGEXP_EXTRACT(Other_Data, r'u1=(\d+);') CID, User_ID, RegDate,FTDDate FROM `ddm_dt_v2_us.activity_1684` a inner join ddm_dt_v2_us.match_table_activity_cats_1684 b on a.Activity_ID = b.Activity_ID and EXTRACT(date FROM TIMESTAMP_MICROS( a.Event_Time ) ) = b. _DATA_DATE left join ( select (REGEXP_EXTRACT(Other_Data, r'u1=(\d+);'))  CID, min(  case when Activity_Type='reg_pix' then TIMESTAMP_MICROS( Event_Time ) else null end  ) as RegDate, min(  case when Activity_Type='ftd_pixe' then TIMESTAMP_MICROS( Event_Time ) else null end ) as FTDDate, CURRENT_TIMESTAMP() as LastUpdateDate  from  `ddm_dt_v2_us.activity_1684_*` a inner join ddm_dt_v2_us.match_table_activity_cats_1684_20160814 b on a.Activity_ID = b.Activity_ID where Activity_Type in ('reg_pix', 'ftd_pixe') group by  CID ) RegFTD on REGEXP_EXTRACT(Other_Data, r'u1=(\d+);') = RegFTD.CID WHERE User_ID <>"0" and REGEXP_EXTRACT(Other_Data, r'u1=(\d+);') <>"0" and  Activity_Type  in ('reg_pix','ftd_pixe','appin0','depos0','login') UNION ALL SELECT REGEXP_EXTRACT(U_Value, r'cid\*\*(\d+),') CID, User_ID, RegDate,FTDDate FROM `ddm_dt_v2_us.impression_1684_*` left join (select   (REGEXP_EXTRACT(Other_Data, r'u1=(\d+);'))  CID, min(  case when Activity_Type='reg_pix' then TIMESTAMP_MICROS( Event_Time ) else null end  ) as RegDate, min(  case when Activity_Type='ftd_pixe' then TIMESTAMP_MICROS( Event_Time ) else null end ) as FTDDate, CURRENT_TIMESTAMP() as LastUpdateDate from  `ddm_dt_v2_us.activity_1684_*` a       inner join ddm_dt_v2_us.match_table_activity_cats_1684_20160814 b on a.Activity_ID = b.Activity_ID where Activity_Type in ('reg_pix', 'ftd_pixe') group by  CID                   ) RegFTD on REGEXP_EXTRACT(U_Value, r'cid\*\*(\d+),') = RegFTD.CID WHERE User_ID <>"0" and REGEXP_EXTRACT(U_Value, r'cid\*\*(\d+),') <> "0" ) " 
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



