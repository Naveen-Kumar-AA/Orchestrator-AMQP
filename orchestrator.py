from or_fns import *
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS


if __name__ == "__main__":
    
    
    next_job = threading.Thread(target=retriveNextJob)
    chk_ping = threading.Thread(target= checkPing)

    app = Flask(__name__)
    cors = CORS(app, resources={r"/*": {"origins": "*"}})

    
    next_job.start()    
    
    chk_ping.start()

    # while(True):
        # getScheduleInput()
    
    @app.route('/schedule', methods=["POST"])
    def schedule():
        # json_object = request.get_json()
        body = request.json
        table_name = "_SCHEDULE_INFO"
        insertInto(table_name, [body.get("job_name"), body.get("no_of_occurence"), body.get("time_interval"), body.get("queue_id"), str(int(time.time()))])
        return "Job Scheduled!"

    @app.route('/deploy-job', methods=["POST"])
    def deployJob():
        body = request.json
        # file_name = input("Enter file name : ")
        # uniq_job_id = input("Enter unique job ID : ")
        # bin_data = open(file_name, 'rb').read()
        # print(bin_data)
        job_id = body.get("job_id")
        job_content = body.get("job_content")

        bin_data = job_content.encode()
        hex_data = codecs.encode(bin_data, "hex_codec")
        
        hex_data = hex_data.decode()
        # print(hex_data)
        #hex_data = hex_data.encode()
        #print(hex_data)
        #hex_data = codecs.decode(hex_data, "hex_codec")
        #print(hex_data)
        
        input_list = [job_id, hex_data]
        # print(input_list)
        insertJobInto("_DEPLOYED_JOBS", input_list)
        
        return "Job Deployed!"
    @app.route('/get-job-content-by-job-id', methods=["POST"])
    def getJobContent():
        body = request.json
        job = getJobContentByJobID(body.get("job_id"))
        print(job)
        return jsonify({"job" : job})


    @app.route('/get-execution-result', methods=["GET"])
    def getExecutionResult():
        # json_object = request.get_json()
        result = getExecResult()
        # print(result)

        # return {"result" : result}
        return jsonify({"result" : result})        

    
    app.run(debug=True, port=8080)

    # wait until thread 1 is completely executed
    next_job.join()

    chk_ping.join()
    
