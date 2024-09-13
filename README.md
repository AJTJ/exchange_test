## Some notes on this project.

- I've tested this with some unit tests and a CSV file of edge cases such as: duplicate transactions, invalid disputes/chargebacks, insufficient funds for withdrawals, precision errors etc... It's quite an interesting technical exam, as there are numerous "gotchas". In production, I'd want a lot more unit tests and edge case exploration.

- The separation of concerns between account manipulation and transaction validation/management was a design choice. In my mind it is idiomatic to rustic systems to maintain a more functional structure. And I find that modularity helps with maintainability and feature enhancements.

- For error handling, the code handles various types of errors gracefully, for now I have for the most part, decided to simply log errors to `eprintln!`. In production I would include more robust error handling and mechanisms such as retry logic, and different levels of error handling.

- I've attempted to leave some comments in my code, which is usually a preferred form of documentation.

- Currently I'm storing accounts, transactions, and disputes in memory. This is efficient for a demo, but obviously wouldn't do at scale. You would be using a persistent database solution, and perhaps you would explore partitioning by `client_id` in order to scale efficiently.

- My code streams the CSV file one line at a time, rather than loading the whole CSV file into memory. For demo purposes, this handles very large CSV files, but at scale I'd continue with the idea of partitioning by `client_id` in order to have multiple services (horizontal scaling) processing different partitions concurrently. Avoiding conflicts and maintaining transaction ordering for each client would be a priority.

- Regarding security, I've put a bit of care into ensuring proper precision and rejecting transactions that exceed it. In a real world scenario, overflow and underflow would likely need further scrutiny.