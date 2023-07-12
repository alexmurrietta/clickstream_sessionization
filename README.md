# clickstream_sessionization
NOTE***: Certain information has been redacted/changed to protect proprietary information

Created session logic using SQL for users in the TurboTax &amp; Quickbooks Live products. With this SQL script, I created an automated job using SuperGlue, which refreshes the table daily in the AWS datalake to be consumed by other analysts

## Use Case
Users in these products are constantly going from one section of the product to the other. This behavior is tracked using clickstream, which produces hundreds of millions, and sometimes billions, of data points. The goal was to create a framework which tracks user behavior in a certain area of the product, and creates unique sessions for each interaction

## Methodology
The main difficulty of creating sessions in this example is the necessity of chronology. We need to know each time a user enters and exits a certain part of the product & when they do it. In this case, order matters

Since order matters, the main way to develop sessions is through window functions. Using functions such as row_number(), lead() & lag(), we can flag each time an Expert enters and exits the area & the timestamps related to it. The final output is a table with every session that occurs in that area of the platform, it's start and end, the user who experienced it, and other relevant dimensions
