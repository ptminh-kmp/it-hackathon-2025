{
  "Comment": "State machine to process a batch of CLEANED service logs, aggregate findings, query Knowledge Base, invoke LLM for prediction, and send results to EC2 API.",
  "StartAt": "ListCleanLogFiles",
  "States": {
    "ListCleanLogFiles": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:YOUR_AWS_REGION:YOUR_ACCOUNT_ID:function:YourPrefix-ListCleanLogFilesLambda",
      "Parameters": {
        "BatchID.$": "$.BatchID",                 // Input từ S3BatchTriggerLambda
        "S3Bucket.$": "$.S3Bucket",               // Input từ S3BatchTriggerLambda
        "BatchFolderPath.$": "$.BatchFolderPath"  // Input từ S3BatchTriggerLambda
      },
      "ResultPath": "$.ListFilesOutput", // Lưu kết quả của Lambda này vào key "ListFilesOutput" trong state data
                                        // State data bây giờ sẽ là input gốc + key "ListFilesOutput"
      "Next": "ProcessEachCleanLogFile",
      "Catch": [
        {
          "ErrorEquals": [ "States.ALL" ], // Bắt tất cả các lỗi từ Task này
          "Next": "NotifyBatchFailure",    // Nếu lỗi, đi đến state NotifyBatchFailure
          "ResultPath": "$.ErrorDetails"   // Lưu chi tiết lỗi vào key "ErrorDetails"
        }
      ]
    },
    "ProcessEachCleanLogFile": {
      "Type": "Map",
      "ItemsPath": "$.ListFilesOutput.CleanLogFiles", // Lặp qua mảng "CleanLogFiles" bên trong "ListFilesOutput"
      "MaxConcurrency": 10, // Số lượng WorkerLambda chạy song song (điều chỉnh nếu cần)
      "Parameters": { // Input cho mỗi lần gọi WorkerLambda trong vòng lặp Map
        "BatchID.$": "$.ListFilesOutput.BatchID",          // Lấy BatchID từ output của ListCleanLogFiles
        "S3Bucket.$": "$.ListFilesOutput.S3Bucket",        // Lấy S3Bucket từ output của ListCleanLogFiles
        "CleanLogFile.$": "$$.Map.Item.Value"             // "$$.Map.Item.Value" là object hiện tại trong mảng CleanLogFiles (ví dụ: {"S3Key": "...", "ServiceNameFromFile": "..."})
      },
      "Iterator": { // Định nghĩa một state machine con để chạy cho mỗi item trong ItemsPath
        "StartAt": "InvokeWorkerCleanLogLambda",
        "States": {
          "InvokeWorkerCleanLogLambda": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:YOUR_AWS_REGION:YOUR_ACCOUNT_ID:function:YourPrefix-WorkerCleanLogLambda",
            "TimeoutSeconds": 300, // Timeout cho mỗi worker (5 phút)
            "Retry": [ {         // Cấu hình retry nếu worker thất bại
              "ErrorEquals": [ "States.TaskFailed", "States.Timeout", "Lambda.ServiceException", "Lambda.TooManyRequestsException", "Lambda.SdkClientException"],
              "IntervalSeconds": 15, // Thời gian chờ giữa các lần retry
              "MaxAttempts": 2,      // Số lần retry tối đa
              "BackoffRate": 1.5     // Hệ số tăng thời gian chờ
            } ],
            "ResultPath": null, // Bỏ qua output của WorkerLambda, không thêm vào mảng kết quả của Map state
                                // vì WorkerLambda đã ghi kết quả vào DynamoDB.
            "End": true         // Kết thúc nhánh của Map state (iterator)
          }
        }
      },
      // Output của Map state này sẽ là input của nó (do ResultPath: null ở Iterator và ở đây)
      // hoặc một mảng các output từ Iterator nếu ResultPath của Map không phải null.
      // Vì Iterator có ResultPath: null, và Map state này cũng ResultPath: null,
      // nên output của Map state này sẽ là input của nó (tức là toàn bộ state data trước đó).
      "ResultPath": null, 
      "Next": "CheckIfAnyFilesProcessed", // Chuyển sang bước kiểm tra sau khi Map hoàn thành
      "Catch": [ // Bắt lỗi từ Map state (ví dụ: nếu một iteration thất bại và không retry được nữa)
        {
          "ErrorEquals": [ "States.ALL" ],
          "Next": "NotifyBatchFailure",
          "ResultPath": "$.ErrorDetails"
        }
      ]
    },
    "CheckIfAnyFilesProcessed": { // State mới để kiểm tra xem có file nào được xử lý không
      "Type": "Choice",
      "Choices": [
        {
          // Kiểm tra xem ListFilesOutput.CleanLogFiles có rỗng không
          // States.ArrayLength không hoạt động trực tiếp trên null, nên cần kiểm tra ListFilesOutput trước
          "Variable": "$.ListFilesOutput.CleanLogFiles",
          "IsPresent": true, // Đảm bảo ListFilesOutput.CleanLogFiles tồn tại
          "Next": "CheckFileArrayNotEmpty"
        }
      ],
      "Default": "NoFilesToProcess" // Nếu ListFilesOutput.CleanLogFiles không tồn tại (lỗi ở ListCleanLogFiles)
    },
    "CheckFileArrayNotEmpty":{
        "Type": "Choice",
        "Choices": [
            {
              "Variable": "$.ListFilesOutput.CleanLogFiles[0]", // Kiểm tra phần tử đầu tiên có tồn tại không
              "IsPresent": true,
              "Next": "InvokeAggregationCleanLogLambda" // Nếu có file, đi đến Aggregation
            }
        ],
        "Default": "NoFilesToProcess" // Nếu mảng CleanLogFiles rỗng
    },
    "NoFilesToProcess": { // State nếu không có file nào được list để xử lý
        "Type": "Pass",
        "Result": {
            "status": "SKIPPED_NO_FILES_LISTED",
            "message": "No clean log files were listed for processing in the batch.",
            "BatchID.$": "$.ListFilesOutput.BatchID" // Hoặc "$.BatchID" nếu ListFilesOutput không có
        },
        "Next": "NotifySkipped" // Hoặc End nếu không cần thông báo
    },
    "InvokeAggregationCleanLogLambda": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:YOUR_AWS_REGION:YOUR_ACCOUNT_ID:function:YourPrefix-AggregationCleanLogLambda",
      "Parameters": {
        // Truyền toàn bộ output của "ListCleanLogFiles" state (được lưu trong $.ListFilesOutput)
        // vào key "ListFilesResult" của input cho AggregationCleanLogLambda
        "ListFilesResult.$": "$.ListFilesOutput" 
      },
      "ResultPath": "$.AggregationOutput", // Lưu kết quả của AggregationLambda vào key "AggregationOutput"
      "Next": "NotifyBatchSuccess",
      "Catch": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "Next": "NotifyBatchFailure",
          "ResultPath": "$.ErrorDetails"
        }
      ]
    },
    "NotifyBatchSuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish", // Service integration với SNS
      "Parameters": {
        "TopicArn": "arn:aws:sns:YOUR_AWS_REGION:YOUR_ACCOUNT_ID:YourSuccessSNSTopicName", // THAY THẾ
        "Message.$": "States.Format('Batch processing for BatchID [{}] completed successfully. Aggregation message: {}. View execution: {}', $.ListFilesOutput.BatchID, States.StringToJson($.AggregationOutput.body).message, $$.Execution.Id)",
        "Subject.$": "States.Format('SUCCESS: AI Batch Prediction for BatchID {}', $.ListFilesOutput.BatchID)"
      },
      "End": true
    },
    "NotifySkipped": { // State mới để thông báo nếu không có file nào được xử lý
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:YOUR_AWS_REGION:YOUR_ACCOUNT_ID:YourSuccessSNSTopicName", // Có thể dùng topic success hoặc topic riêng
        "Message.$": "States.Format('Batch processing for BatchID [{}] was skipped because no clean log files were found to process. Execution: {}', $.BatchID, $$.Execution.Id)",
        // Lưu ý: $.BatchID ở đây là input gốc của state machine nếu ListFilesOutput không tồn tại.
        // Nếu ListFilesOutput tồn tại nhưng CleanLogFiles rỗng, dùng $.ListFilesOutput.BatchID.
        // Để an toàn hơn, có thể cần một Pass state trước đó để chuẩn bị BatchID cho message này.
        // Ví dụ đơn giản:
        // "Message.$": "States.Format('Batch processing SKIPPED for BatchID (from SFN input) [{}]. No files listed. Execution: {}', $.BatchID, $$.Execution.Id)",
        "Subject.$": "States.Format('SKIPPED: AI Batch Prediction for BatchID {} - No Files', $.BatchID)"
      },
      "End": true
    },
    "NotifyBatchFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:YOUR_AWS_REGION:YOUR_ACCOUNT_ID:YourFailureSNSTopicName", // THAY THẾ
        // Cố gắng lấy BatchID từ ListFilesOutput nếu có, nếu không thì từ input gốc của SFN
        "Message.$": "States.Format('Batch processing FAILED. BatchID [{}]. Execution ARN: {}. Error Details: {}', States. lựa chọn($.ListFilesOutput.BatchID, $.BatchID, 'UNKNOWN'), $$.Execution.Id, States.JsonToString($.ErrorDetails))",
        "Subject.$": "States.Format('FAILED: AI Batch Prediction for BatchID {}', States. lựa chọn($.ListFilesOutput.BatchID, $.BatchID, 'UNKNOWN'))"
      },
      "End": true
    }
  }
}
