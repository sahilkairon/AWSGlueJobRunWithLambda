package com.amazonaws.lambda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.StartJobRunRequest;
import com.amazonaws.services.glue.model.StartJobRunResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class UserBulkDataS3ToGlue implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        try {

            AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(Regions.AP_SOUTH_1).build();
            context.getLogger().log("AWS Glue client created");
            
            StartJobRunRequest job = new StartJobRunRequest();
            job.setJobName("userwithpwdRDS");
            
            context.getLogger().log("AWS Glue job run request set");
            
            StartJobRunResult jobResult = glue.startJobRun(job);
            
            context.getLogger().log("Job executed successfully");
            context.getLogger().log(jobResult.toString());
       
       
             return jobResult.toString();
             }catch(Exception e){
             	context.getLogger().log("Exception occured :- " + e.getMessage());
             	return "Exception occured :- " + e.getMessage();
             }
    }

}
