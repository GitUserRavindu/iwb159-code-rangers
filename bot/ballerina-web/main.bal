    import ballerina/http;
    import ballerina/io;
    import ballerinax/mongodb;
    import ballerina/lang.value;


    // MongoDB Atlas configuration
    configurable string connection_string = "mongodb+srv://yutharsan:0585@cluster0.z8x37.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";
    configurable string database = "test";
    configurable string qa_collection_name = "qa_documents";
    configurable string category_collection_name = "category";

    mongodb:Client mongoDb = check new ({
        connection: connection_string
    });


    public type Post record {|
        json _id;
        string userName;
        string postMessage;
        string postDate;
        Reply[] replies;
    |};

    public type Reply record {|
        string userName;
        string replyMessage;
        string replyDate;
    |};

    // public type PostTest record {|
    //     string userName;
    //     string postMessage;
    //     string postDate;
    //     Reply[] replies;
    // |};

    //core configure
    @http:ServiceConfig {
        cors: {
            allowOrigins: ["*"],
            allowHeaders: ["Content-Type"],
            allowMethods: ["GET", "POST", "OPTIONS"]
        }
    }
    // Define the service
    service /chatbot on new http:Listener(8080) {

        // Resource function to handle POST requests to the /chatbot endpoint
        resource function post .(http:Caller caller, http:Request req) returns error? {
            // Retrieve the message from the request payload
            json payload = check req.getJsonPayload();
            string message = (check payload.message).toString(); // Get the user's message
            io:println("Sending message to Python chatbot: " + message);

            // Call the Python server with the message and get the response
            json response = (check callPythonChatbot(message)).toString();
            io:println("response: ", response);

            // Send the response back to the client
            check caller->respond(response);

        }

        // Resource function to handle POST requests to the /getContent endpoint
        resource function post getContent(http:Caller caller, http:Request req) returns error? {
            json payload = check req.getJsonPayload();
            string message = (check payload.message).toString();

            // Execute the SQL query
            // stream<record {|anydata...;|}, sql:Error?> resultStream = pool->query(`SELECT * FROM content`);
            // json[] resultJson = [];

            // // Process the result stream and build the JSON response
            // check resultStream.forEach(function(record {|anydata...;|} row) {
            //     map<json> rowJson = {};
            //     foreach var [key, value] in row.entries() {
            //         rowJson[key] = <json>value;
            //     }
            //     resultJson.push(rowJson);
            // });
            // json resultJson = check getContent(message);

            json result = check testVectorSearch(message);
            json resultJson = result;

            // Send the result back to the client
            check caller->respond(resultJson);
        }

        // Resource function to handle POST requests to the /getInstruction endpoint
        resource function post getInstruction(http:Caller caller, http:Request req) returns error? {
            json payload = check req.getJsonPayload();
            string message = (check payload.message).toString();

            json result = check testCategorySearch(message);
            json resultJson = result;

            // Send the result back to the client
            check caller->respond(resultJson);
        }

        resource function get allposts(http:Caller caller) returns error? {

            // Access the database and collection using name
            mongodb:Database db = check mongoDb->getDatabase("test");
            mongodb:Collection postCollection = check db->getCollection("community_posts");

            Post[] response = check getPosts(postCollection);

            // Convert the stream to a JSON array
            json responseJson = value:toJson(response);

            // Logging to verify
            io:println("response: ", responseJson);

            // Send to frontend
            check caller->respond(responseJson);
        }

        resource function post addReply(http:Caller caller, http:Request req) returns error? {
            json payload = check req.getJsonPayload();
            json post_id = check payload._id;
            // string postId = check post_id.oid;
            Reply newReply = {
                userName: check payload.userName,
                replyMessage: check payload.replyMessage,
                replyDate: check payload.replyDate
            };

            // Access the database and collection
            mongodb:Database db = check mongoDb->getDatabase("test");
            mongodb:Collection postCollection = check db->getCollection("community_posts");

            boolean result = check addReplyToPost(postCollection, post_id, newReply);

            json response = { success: result };
            check caller->respond(response);
        }
    }

    // }

    // Function to call the Python chatbot

    function callPythonChatbot(string message) returns json|error {
        // Define the Python chatbot API URL
        string pythonApiUrl = "http://localhost:5002";
        json payload = {"message": message}; // Prepare the request payload

        // Create a new HTTP client for the Python API
        http:Client pythonClient = check new (pythonApiUrl);

        // Log the payload to verify what is being sent
        io:println("Sending payload to Python chatbot: ", payload.toString());

        // Send a POST request to the Python chatbot
        http:Response response = check pythonClient->post("/chatbot", payload);

        // Log the entire response body as a string to see if it's valid JSON or not
        string responseBody = (check (check response.getJsonPayload()).result).toString();

        // string responseMessage = check response.response;

        io:println("Raw response from Python chatbot: ", responseBody);

        // Check if the response is JSON. If not, log an error message.
        json jsonResponse;
        if response.getContentType() == "application/json" {
            // Parse the response to JSON only if it's of type application/json
            jsonResponse = check (check response.getJsonPayload()).result;

        } else {
            io:println("Error: Received a non-JSON response from the Python server.");
            return error("Non-JSON response: " + responseBody);
        }

        return jsonResponse;

    }

    function parseEmbedding(json embeddingJson) returns float[]|error {
        float[] embedding = [];
        foreach var value in <json[]>embeddingJson {
            embedding.push(<float>value);
        }
        return embedding;
    }

    function generateEmbedding(string text) returns float[]|error {
        // Calling embedding endpoint
        // For running embedding locally, first run 'pip install -r requirements.txt'
        // Create run a server api using 'uvicorn embedding_api:app --host 0.0.0.0 --port 8000'
        // Change the link 'https://embedding-api-agro-bot.onrender.com' -> 'http://localhost:8000'
        http:Client embeddingClient = check new ("https://embedding-api-agro-bot.onrender.com"); // deployement on the web very slow because of free tier
        json payload = { "text": text };
        http:Response resp = check embeddingClient->post("/embed", payload);
        json respJson = check resp.getJsonPayload();
        float[] embedding = check parseEmbedding(check respJson.embedding);
        return embedding;
    }


    function findSimilarAnswers(float[] queryEmbedding, mongodb:Database database, mongodb:Collection dataCollection) returns QAResult[]|error {
        // Mongo db query
        map<json>[] pipeline = [
            {
                "$vectorSearch": {
                "index": "index1",
                "path": "embedding",
                "queryVector": queryEmbedding,
                "numCandidates": 3,
                "limit": 1
                }
            }
        ];

        // The aggregate method return a stream containg results
        stream<QAResult, error?> resultStream = check dataCollection->aggregate(pipeline);

        QAResult[] results = [];

        error? e = resultStream.forEach(function(json doc) {
                do {
                    // Access the 'question' and 'answer' fields from the JSON document
                    string question = check value:ensureType(doc.question, string);
                    string answer = check value:ensureType(doc.answer, string);

                    QAResult qaResult = { question: question, answer: answer };
                    results.push(qaResult);
                } on fail var err {
                    io:println("Error processing document: ", err.message());
                }

                return;
            }
        );

        if e is error {
            return e;
        }

        return results;
    }

    function findCategory(string userInput, mongodb:Database datstringabase, mongodb:Collection dataCollection) returns CategoryDocument[]|error {
        // Mongo db query
        map<json>[] pipeline = [
            {
                "$search": {
                "index": "category_search",  // Specify the index name. "default" is the name if you haven't created a custom one.
                "text": {
                    "query": userInput,  // The search term (with potential typos).
                    "path": "category", // The field to search within.
                    "fuzzy": {
                    "maxEdits": 1,  // Allow up to 2 edits (insertions, deletions, or substitutions).
                    "prefixLength": 3 // The number of initial characters that must match exactly.
            }
        }
        }
    }
        ];

        // The aggregate method return a stream containg results
        stream<CategoryDocument, error?> resultStream = check dataCollection->aggregate(pipeline);

        CategoryDocument[] results = [];

        error? e = resultStream.forEach(function(json doc) {
                do {
                    // Access the 'question' and 'answer' fields from the JSON document
                    string category = check value:ensureType(doc.category, string);
                    string content = check value:ensureType(doc.content, string);

                    CategoryDocument caResult = { category: category, content: content};
                    results.push(caResult);
                } on fail var err {
                    io:println("Error processing document: ", err.message());
                }

                return;
            }
        );

        if e is error {
            return e;
        }

        return results;
    }

    function testCategorySearch(string userInput) returns json|error {
        // Access the database and collection using name
        mongodb:Database qa_database = check mongoDb->getDatabase(database);
        mongodb:Collection dataCollection = check qa_database->getCollection(category_collection_name);

        // Test finding similar answers
        CategoryDocument[] similarDocs = check findCategory(userInput, qa_database, dataCollection);

        // Convert the similarAnswers array to JSON
        json similarAnswersJson = value:toJson(similarDocs);

        // Print the JSON (optional)
        io:println(similarAnswersJson);

        return similarAnswersJson;
    }
    function testVectorSearch(string userInput) returns json|error {
        // Access the database and collection using name
        mongodb:Database qa_database = check mongoDb->getDatabase(database);
        mongodb:Collection dataCollection = check qa_database->getCollection(qa_collection_name);

        // Test finding similar answers
        float[] queryEmbedding = check generateEmbedding(userInput);
        QAResult[] similarAnswers = check findSimilarAnswers(queryEmbedding, qa_database, dataCollection);

        // Convert the similarAnswers array to JSON
        json similarAnswersJson = value:toJson(similarAnswers);

        // Print the JSON (optional)
        io:println(similarAnswersJson);

        return similarAnswersJson;
    }

    function getPosts(mongodb:Collection postCollection) returns Post[]|error {
        // Get all posts from db
        stream<Post, error?> resultStream = check postCollection->find();

        Post[] results = [];

        error? e = resultStream.forEach(function(json doc) {
            do {
                // Access the 'question' and 'answer' fields from the JSON document
                string userName = check value:ensureType(doc.userName, string);
                string answer = check value:ensureType(doc.postMessage, string);
                string postDate = check value:ensureType(doc.postDate, string);
                Reply[] replies = check getReplies(doc.replies);



                // Check if 'replies' field exists and is of type json[]


                Post result = { _id: check doc._id ,userName: userName, postMessage: answer, postDate: postDate, replies: replies};

                results.push(result);
            } on fail var err {
                io:println("Error processing document: ", err.message());
            }

            return;
        });

        if e is error {
            return e;
        }
        return results;
    }



    type QADocument record {|
        string question;
        string answer;
        float[] embedding;
    |};

    type QAResult record {|
        string question;
        string answer;
    |};

    type CategoryDocument record {|
        string category;
        string content;
    |};



    function getReplies(json|error? replyStream) returns Reply[]|error {
        Reply[] results = [];

        if (replyStream is error) {
            return replyStream;
        }

        if (replyStream is json[]) {
            foreach json doc in replyStream {
                do {
                    // Access the 'userName', 'replyMessage', and 'replyDate' fields from the JSON document
                    string userName = check value:ensureType(doc.userName, string);
                    string answer = check value:ensureType(doc.replyMessage, string);
                    string replyDate = check value:ensureType(doc.replyDate, string);

                    Reply result = { userName: userName, replyMessage: answer, replyDate: replyDate };
                    results.push(result);
                } on fail var err {
                    io:println("Error processing document: ", err.message());
                }
            }
        }

        return results;
    }
    // function addReplyToPost(mongodb:Collection collection, json postId, Reply newReply) returns boolean|error {
    //     map<json> filter = { "_id": postId };

    //     mongodb:Update updateDoc = {
    //         set: {
    //             "$push": {
    //                 "replies": {
    //                     "userName": newReply.userName,
    //                     "replyMessage": newReply.replyMessage,
    //                     "replyDate": newReply.replyDate
    //                 }
    //             }
    //         }
    //     };

    //     mongodb:UpdateOptions options = {
    //         upsert: true
    //     };

    //     mongodb:UpdateResult result = check collection->updateOne(filter, updateDoc, options);

    //     return result.modifiedCount > 0;
    // }

    function addReplyToPost(mongodb:Collection collection, json postId, Reply newReply) returns boolean|error {
    // First, retrieve the current document
    Post? existingDoc = check collection->findOne({ "_id": postId });

    Reply[] currentReplies = [];

    currentReplies = check existingDoc["replies"].cloneWithType();


    // Add the new reply
    currentReplies.push(newReply);

    // Create the update document
    mongodb:Update updateDoc = {
        set: {
            "replies": currentReplies
        }
    };

    mongodb:UpdateOptions options = {
        upsert: true
    };

    mongodb:UpdateResult result = check collection->updateOne({ "_id": postId }, updateDoc, options);

    return result.modifiedCount > 0 || result.upsertedId != ();
}
