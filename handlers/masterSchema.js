import { DynamoDBClient, CreateTableCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";

const REGION = process.env.REGION;
const STAGE = process.env.STAGE || "dev";
const META_SYNC_LISTING_TABLE = process.env.META_SYNC_LISTING_TABLE;

const ddbClient = new DynamoDBClient({ region: REGION });

export const handler = async (event) => {
    try {
        const body = typeof event.body === "string" ? JSON.parse(event.body) : event.body || {};

        // Step 1: Validate input
        if (!body.tableName || typeof body.tableName !== "string") {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Missing or invalid field: tableName" }),
            };
        }

        if (!Array.isArray(body.attributes) || body.attributes.length === 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Missing or invalid field: attributes (must be non-empty array)" }),
            };
        }

        const targetTableName = `${body.tableName}-${STAGE}`;

        // Step 2: Extract keys
        const partitionKey = body.attributes.find(attr => attr.partionkey === true);
        if (!partitionKey) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Partition key (partionkey: true) is required" }),
            };
        }

        const sortKey = body.attributes.find(attr => attr.sortKey === true);

        // Step 3: Build AttributeDefinitions
        const attributeDefinitions = body.attributes
            .filter(attr => attr.partionkey || attr.sortKey)
            .map(attr => ({
                AttributeName: attr.column,
                AttributeType: attr.type?.toUpperCase().startsWith("STRING") ? "S" :
                    attr.type?.toUpperCase().startsWith("NUMBER") ? "N" : "B",
            }));

        // Step 4: Build KeySchema
        const keySchema = [
            { AttributeName: partitionKey.column, KeyType: "HASH" },
        ];
        if (sortKey) {
            keySchema.push({ AttributeName: sortKey.column, KeyType: "RANGE" });
        }

        // Step 5: Create the DynamoDB table
        const createTableParams = {
            TableName: targetTableName,
            AttributeDefinitions: attributeDefinitions,
            KeySchema: keySchema,
            BillingMode: "PAY_PER_REQUEST",
        };

        console.log("Creating Table with params:", JSON.stringify(createTableParams, null, 2));
        await ddbClient.send(new CreateTableCommand(createTableParams));

        // Step 6: Insert into meta-sync table
        const createdAt = new Date().toISOString();
        const allFields = JSON.stringify(body.attributes);
        const requiredFields = JSON.stringify(
            body.attributes.filter(attr => attr.required === true)
        );

        const metaItemParams = {
            TableName: META_SYNC_LISTING_TABLE,
            Item: {
                tableName: { S: targetTableName },
                createdAt: { S: createdAt },
                allFields: { S: allFields },
                requiredFields: { S: requiredFields },
                status: { N: "1" },
                updatedAt: { S: createdAt },
            },
        };

        await ddbClient.send(new PutItemCommand(metaItemParams));

        // Step 7: Return success
        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": true,
            },
            body: JSON.stringify({
                message: "Table created successfully",
                tableName: targetTableName,
                defaults: {
                    status: 1,
                    createdAt,
                    updatedAt: createdAt,
                },
            }),
        };
    } catch (error) {
        console.error("Error creating table:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Internal server error",
                error: error.message || error,
            }),
        };
    }
};
