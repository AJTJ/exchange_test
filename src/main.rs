use csv::ReaderBuilder;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io;
use std::str::FromStr;

#[derive(Debug)]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl FromStr for TxType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "deposit" => Ok(TxType::Deposit),
            "withdrawal" => Ok(TxType::Withdrawal),
            "dispute" => Ok(TxType::Dispute),
            "resolve" => Ok(TxType::Resolve),
            "chargeback" => Ok(TxType::Chargeback),
            _ => Err("Unknown transaction type"),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
struct Record {
    #[serde(rename = "type")]
    tx_type: String,
    client: u16,
    tx: u32,
    #[serde(deserialize_with = "csv::invalid_option")]
    amount: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct Account {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

impl Account {
    fn new() -> Account {
        Account {
            available: Decimal::new(0, 0),
            held: Decimal::new(0, 0),
            total: Decimal::new(0, 0),
            locked: false,
        }
    }

    fn deposit(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.locked {
            return Err("Deposit error: Account is locked");
        }
        self.available += amount;
        self.total += amount;
        Ok(())
    }

    fn withdraw(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.locked {
            return Err("Withdrawal error: Account is locked");
        }
        if self.available >= amount {
            self.available -= amount;
            self.total -= amount;
            Ok(())
        } else {
            Err("Withdrawal error: Insufficient funds for withdrawal")
        }
    }

    fn apply_dispute(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.locked {
            return Err("Dispute error: Account is locked");
        }
        if self.available >= amount {
            self.available -= amount;
            self.held += amount;
            Ok(())
        } else {
            Err("Dispute error: Insufficient available funds for dispute")
        }
    }

    fn resolve_dispute(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.locked {
            return Err("Resolve error: Account is locked");
        }
        if self.held >= amount {
            self.held -= amount;
            self.available += amount;
            Ok(())
        } else {
            Err("Resolve error: Insufficient held funds for resolve")
        }
    }

    fn chargeback(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.locked {
            return Err("Chargeback error: Account is locked");
        }
        if self.held >= amount {
            self.total -= amount;
            self.held -= amount;
            self.locked = true;
            Ok(())
        } else {
            Err("Chargeback error: Insufficient held funds for chargeback")
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: cargo run -- <input_csv>");
        std::process::exit(1);
    }
    let input_file = &args[1];

    let file = File::open(input_file)?;
    let mut rdr = ReaderBuilder::new().comment(Some(b'#')).from_reader(file);

    // For the purposes of this exercise, we'll store accounts, transactions, and disputes in memory
    let mut accounts: HashMap<u16, Account> = HashMap::new();
    let mut transactions: HashMap<u32, Record> = HashMap::new();
    let mut disputes: HashSet<u32> = HashSet::new();

    // Stream the CSV file to avoid loading the entire file into memory
    for result in rdr.deserialize() {
        let record: Record = result?;
        if let Err(e) =
            process_transaction(&record, &mut accounts, &mut transactions, &mut disputes)
        {
            // In the specification we are told to ignore invalid disputes, resolves, and chargebacks
            // so we'll just print an error message and continue processing the rest of the transactions
            // For a project of larger scale we would use a logging library to log non-critical errors vs critical errors
            eprintln!("Failed to process transaction: {}", e);
        }
    }

    write_accounts_to_csv(&accounts)?;
    Ok(())
}

fn process_transaction(
    record: &Record,
    accounts: &mut HashMap<u16, Account>,
    transactions: &mut HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    let tx_type = TxType::from_str(&record.tx_type)?;

    match tx_type {
        TxType::Deposit => {
            let mut account = accounts
                .get(&record.client)
                .cloned()
                .unwrap_or_else(Account::new);

            process_deposit(&record, &mut account, transactions)?;

            // Add OR Update the account if the deposit was successful
            accounts.insert(record.client, account.clone());

            Ok(())
        }
        TxType::Withdrawal => {
            if let Some(account) = accounts.get_mut(&record.client) {
                process_withdrawal(&record, account, transactions)
            } else {
                Err(format!(
                    "Account {} does not exist for transaction type {:?}",
                    record.client, tx_type
                )
                .into())
            }
        }
        TxType::Dispute | TxType::Resolve | TxType::Chargeback => {
            if let Some(account) = accounts.get_mut(&record.client) {
                match tx_type {
                    TxType::Dispute => process_dispute(&record, account, transactions, disputes),
                    TxType::Resolve => process_resolve(&record, account, transactions, disputes),
                    TxType::Chargeback => {
                        process_chargeback(&record, account, transactions, disputes)
                    }
                    _ => unreachable!(),
                }
            } else {
                Err(format!(
                    "Account {} does not exist for transaction type {:?}",
                    record.client, tx_type
                )
                .into())
            }
        }
    }
}

fn process_deposit(
    record: &Record,
    account: &mut Account,
    transactions: &mut HashMap<u32, Record>,
) -> Result<(), Box<dyn Error>> {
    if transactions.contains_key(&record.tx) {
        return Err(format!("Duplicate transaction ID: {}; ignoring", record.tx).into());
    }

    if let Some(amount) = record.amount {
        if amount.is_sign_negative() {
            return Err(format!(
                "Deposit amount cannot be negative; transaction {}",
                record.tx
            )
            .into());
        }
        if !has_valid_precision(&amount) {
            return Err(format!(
                "Deposit amount exceeds allowed precision; transaction {}",
                record.tx
            )
            .into());
        }

        account.deposit(amount)?;
        transactions.insert(record.tx, record.clone());
        Ok(())
    } else {
        Err(format!("Deposit transaction {} missing amount", record.tx).into())
    }
}

fn process_withdrawal(
    record: &Record,
    account: &mut Account,
    transactions: &mut HashMap<u32, Record>,
) -> Result<(), Box<dyn Error>> {
    if transactions.contains_key(&record.tx) {
        return Err(format!("Duplicate transation ID: {}; ignoring", record.tx).into());
    }

    if let Some(amount) = record.amount {
        if amount.is_sign_negative() {
            return Err(format!(
                "Withdrawal amount cannot be negative; transaction {}",
                record.tx
            )
            .into());
        }

        if !has_valid_precision(&amount) {
            return Err(format!(
                "Withdrawal amount exceeds allowed precision; transaction {}",
                record.tx
            )
            .into());
        }

        account.withdraw(amount)?;
        transactions.insert(record.tx, record.clone());
        Ok(())
    } else {
        Err(format!("Withdrawal transaction {} missing amount", record.tx).into())
    }
}

fn process_dispute(
    record: &Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    let disputed_tx = match transactions.get(&record.tx) {
        Some(tx) => tx,
        None => return Err(format!("Dispute error: Transaction {} not found", record.tx).into()),
    };

    if disputed_tx.tx_type.to_lowercase() != "deposit" {
        return Err(format!("Dispute error: Transaction {} is not a deposit", record.tx).into());
    }

    if disputes.contains(&record.tx) {
        return Err(format!(
            "Dispute error: Transaction {} is already disputed",
            record.tx
        )
        .into());
    }

    if let Some(amount) = disputed_tx.amount {
        account.apply_dispute(amount)?;
        disputes.insert(record.tx);
        Ok(())
    } else {
        Err(format!("Dispute error: Transaction {} has no amount", record.tx).into())
    }
}

fn process_resolve(
    record: &Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if !disputes.contains(&record.tx) {
        return Err(format!("Resolve error: Transaction {} is not disputed", record.tx).into());
    }

    let disputed_tx = match transactions.get(&record.tx) {
        Some(tx) => tx,
        None => return Err(format!("Resolve error: Transaction {} not found", record.tx).into()),
    };

    if let Some(amount) = disputed_tx.amount {
        account.resolve_dispute(amount)?;
        disputes.remove(&record.tx);
        if disputes.contains(&record.tx) {
            eprintln!(
                "Debug: Dispute on transaction {} was not removed.",
                record.tx
            );
        } else {
            eprintln!(
                "Debug: Dispute on transaction {} successfully removed.",
                record.tx
            );
        }
        Ok(())
    } else {
        Err(format!("Resolve error: Transaction {} has no amount", record.tx).into())
    }
}

fn process_chargeback(
    record: &Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if !disputes.contains(&record.tx) {
        return Err(format!(
            "Chargeback error: Transaction {} is not disputed",
            record.tx
        )
        .into());
    }

    let disputed_tx = match transactions.get(&record.tx) {
        Some(tx) => tx,
        None => {
            return Err(format!("Chargeback error: Transaction {} not found", record.tx).into())
        }
    };

    if let Some(amount) = disputed_tx.amount {
        account.chargeback(amount)?;
        disputes.remove(&record.tx);
        Ok(())
    } else {
        Err(format!("Chargeback error: Transaction {} has no amount", record.tx).into())
    }
}

fn write_accounts_to_csv(accounts: &HashMap<u16, Account>) -> Result<(), Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    wtr.write_record(&["client", "available", "held", "total", "locked"])?;

    for (client_id, account) in accounts {
        wtr.write_record(&[
            client_id.to_string(),
            format!("{:.4}", account.available),
            format!("{:.4}", account.held),
            format!("{:.4}", account.total),
            account.locked.to_string(),
        ])?;
    }

    wtr.flush()?;
    Ok(())
}

fn has_valid_precision(amount: &Decimal) -> bool {
    amount.scale() <= 4 // Scale gives the number of decimal places
}
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::collections::{HashMap, HashSet};
    use std::io::Cursor;

    #[test]
    fn test_large_csv_with_edge_cases() {
        let csv_data = "\
type,client,tx,amount
# Valid Deposits
deposit,1,1,1000.0000
deposit,2,2,2000.1234
deposit,1,3,500.0000
# Invalid Deposit (exceeds precision)
deposit,3,4,100.12345
# Valid Withdrawal
withdrawal,1,5,300.0000
# Withdrawal with Insufficient Funds
withdrawal,2,6,3000.0000
# Duplicate Transaction ID
deposit,1,1,1000.0000
# Dispute on Valid Deposit
dispute,1,3,
# Dispute on Non-existent Transaction
dispute,1,99,
# Dispute on Withdrawal
dispute,1,5,
# Resolve Valid Dispute
resolve,1,3,
# Chargeback on Resolved Dispute
chargeback,1,3,
# Chargeback on Valid Dispute
dispute,2,2,
chargeback,2,2,
# Attempt Transaction on Locked Account
deposit,2,7,500.0000
# Invalid Transaction Type
invalid_type,1,8,100.0000
# Missing Amount in Deposit
deposit,4,9,
# Missing Amount in Withdrawal
withdrawal,4,10,
# Negative Amount in Deposit
deposit,4,11,-100.0000
# Negative Amount in Withdrawal
withdrawal,1,12,-200.0000
# Exceeding Precision in Withdrawal
withdrawal,1,13,50.12345
# Valid Deposit After Chargeback
deposit,1,14,100.0000
";

        let file = Cursor::new(csv_data);
        let mut rdr = ReaderBuilder::new().comment(Some(b'#')).from_reader(file);

        let mut accounts: HashMap<u16, Account> = HashMap::new();
        let mut transactions: HashMap<u32, Record> = HashMap::new();
        let mut disputes: HashSet<u32> = HashSet::new();

        // Keep track of error count
        let mut error_count = 0;

        for result in rdr.deserialize() {
            let record: Record = match result {
                Ok(rec) => rec,
                Err(_) => {
                    error_count += 1;
                    continue;
                }
            };
            if process_transaction(&record, &mut accounts, &mut transactions, &mut disputes)
                .is_err()
            {
                // Increment error count instead of collecting error messages
                error_count += 1;
            }
        }

        // Verify error count
        assert_eq!(error_count, 13); // Expected number of errors

        // Verify the final state of the accounts
        let account1 = accounts.get(&1).unwrap();
        assert_eq!(account1.available, Decimal::new(130000, 2)); // $1300.00
        assert_eq!(account1.held, Decimal::new(0, 2)); // $0.00
        assert_eq!(account1.total, Decimal::new(130000, 2)); // $1300.00
        assert!(!account1.locked); // Account is not locked

        let account2 = accounts.get(&2).unwrap();
        assert_eq!(account2.available, Decimal::new(0, 4)); // $0.0000
        assert_eq!(account2.held, Decimal::new(0, 4)); // $0.0000
        assert_eq!(account2.total, Decimal::new(0, 4)); // $0.0000
        assert!(account2.locked); // Account is locked after chargeback

        // Ensure account 3 and account 4 do not exist due to invalid transactions
        assert!(!accounts.contains_key(&3));
        assert!(!accounts.contains_key(&4));
    }

    // Test basic deposit and withdrawal functionality
    #[test]
    fn test_account_deposit_and_withdrawal() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2)).unwrap(); // Deposit $10.00
        account.withdraw(Decimal::new(500, 2)).unwrap(); // Withdraw $5.00

        assert_eq!(account.available, Decimal::new(500, 2)); // $5.00
        assert_eq!(account.total, Decimal::new(500, 2)); // $5.00
        assert_eq!(account.held, Decimal::new(0, 2)); // $0.00
    }

    // Test dispute and resolve functionality
    #[test]
    fn test_account_dispute_and_resolve() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2)).unwrap(); // Deposit $10.00
        account.apply_dispute(Decimal::new(1000, 2)).unwrap(); // Dispute $10.00
        account.resolve_dispute(Decimal::new(1000, 2)).unwrap(); // Resolve dispute

        assert_eq!(account.available, Decimal::new(1000, 2)); // $10.00
        assert_eq!(account.held, Decimal::new(0, 2)); // $0.00
        assert_eq!(account.total, Decimal::new(1000, 2)); // $10.00
    }

    // Test chargeback functionality
    #[test]
    fn test_account_chargeback() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2)).unwrap(); // Deposit $10.00
        account.apply_dispute(Decimal::new(1000, 2)).unwrap(); // Dispute $10.00
        account.chargeback(Decimal::new(1000, 2)).unwrap(); // Chargeback

        assert_eq!(account.available, Decimal::new(0, 2)); // $0.00
        assert_eq!(account.held, Decimal::new(0, 2)); // $0.00
        assert_eq!(account.total, Decimal::new(0, 2)); // $0.00
        assert!(account.locked); // Account should be locked
    }

    // Test processing a deposit and withdrawal transaction
    #[test]
    fn test_process_transaction_deposit_withdrawal() {
        let mut accounts = HashMap::new();
        let mut transactions = HashMap::new();
        let mut disputes = HashSet::new();

        // Deposit $10.00
        let deposit_record = Record {
            tx_type: "deposit".to_string(),
            client: 1,
            tx: 1,
            amount: Some(Decimal::new(1000, 2)),
        };
        process_transaction(
            &deposit_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        // Withdraw $5.00
        let withdrawal_record = Record {
            tx_type: "withdrawal".to_string(),
            client: 1,
            tx: 2,
            amount: Some(Decimal::new(500, 2)),
        };
        process_transaction(
            &withdrawal_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(500, 2)); // $5.00
        assert_eq!(account.total, Decimal::new(500, 2)); // $5.00
        assert_eq!(account.held, Decimal::new(0, 2)); // $0.00
    }

    // Test processing dispute and resolve transactions
    #[test]
    fn test_process_dispute_and_resolve() {
        let mut accounts = HashMap::new();
        let mut transactions = HashMap::new();
        let mut disputes = HashSet::new();

        // Deposit $10.00
        let deposit_record = Record {
            tx_type: "deposit".to_string(),
            client: 1,
            tx: 1,
            amount: Some(Decimal::new(1000, 2)),
        };
        process_transaction(
            &deposit_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        // Dispute transaction
        let dispute_record = Record {
            tx_type: "dispute".to_string(),
            client: 1,
            tx: 1,
            amount: None,
        };
        process_transaction(
            &dispute_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        // Resolve dispute
        let resolve_record = Record {
            tx_type: "resolve".to_string(),
            client: 1,
            tx: 1,
            amount: None,
        };
        process_transaction(
            &resolve_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(1000, 2)); // $10.00
        assert_eq!(account.held, Decimal::new(0, 2)); // $0.00
        assert_eq!(account.total, Decimal::new(1000, 2)); // $10.00
    }

    // Test processing a chargeback transaction
    #[test]
    fn test_process_chargeback() {
        let mut accounts = HashMap::new();
        let mut transactions = HashMap::new();
        let mut disputes = HashSet::new();

        // Deposit $10.00
        let deposit_record = Record {
            tx_type: "deposit".to_string(),
            client: 1,
            tx: 1,
            amount: Some(Decimal::new(1000, 2)),
        };
        process_transaction(
            &deposit_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        // Dispute transaction
        let dispute_record = Record {
            tx_type: "dispute".to_string(),
            client: 1,
            tx: 1,
            amount: None,
        };
        process_transaction(
            &dispute_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        // Chargeback transaction
        let chargeback_record = Record {
            tx_type: "chargeback".to_string(),
            client: 1,
            tx: 1,
            amount: None,
        };
        process_transaction(
            &chargeback_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        let account = accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(0, 2)); // $0.00
        assert_eq!(account.held, Decimal::new(0, 2)); // $0.00
        assert_eq!(account.total, Decimal::new(0, 2)); // $0.00
        assert!(account.locked); // Account should be locked
    }

    // Test handling duplicate transaction IDs
    #[test]
    fn test_duplicate_transaction_id() {
        let mut accounts = HashMap::new();
        let mut transactions = HashMap::new();
        let mut disputes = HashSet::new();

        // First deposit transaction
        let deposit_record1 = Record {
            tx_type: "deposit".to_string(),
            client: 1,
            tx: 1,
            amount: Some(Decimal::new(1000, 2)),
        };
        process_transaction(
            &deposit_record1,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        // Duplicate deposit transaction with same ID
        let deposit_record2 = Record {
            tx_type: "deposit".to_string(),
            client: 1,
            tx: 1,
            amount: Some(Decimal::new(500, 2)),
        };
        let result = process_transaction(
            &deposit_record2,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        );
        assert!(result.is_err());

        let account = accounts.get(&1).unwrap();
        // Ensure the account balance hasn't changed
        assert_eq!(account.available, Decimal::new(1000, 2)); // $10.00
        assert_eq!(account.total, Decimal::new(1000, 2)); // $10.00
    }

    // Test handling insufficient funds during withdrawal
    #[test]
    fn test_withdrawal_insufficient_funds() {
        let mut accounts = HashMap::new();
        let mut transactions = HashMap::new();
        let mut disputes = HashSet::new();

        // Deposit $5.00
        let deposit_record = Record {
            tx_type: "deposit".to_string(),
            client: 1,
            tx: 1,
            amount: Some(Decimal::new(500, 2)),
        };
        process_transaction(
            &deposit_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        )
        .unwrap();

        // Attempt to withdraw $10.00
        let withdrawal_record = Record {
            tx_type: "withdrawal".to_string(),
            client: 1,
            tx: 2,
            amount: Some(Decimal::new(1000, 2)),
        };
        let result = process_transaction(
            &withdrawal_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        );
        assert!(result.is_err());

        let account = accounts.get(&1).unwrap();
        // Ensure the account balance hasn't changed
        assert_eq!(account.available, Decimal::new(500, 2)); // $5.00
        assert_eq!(account.total, Decimal::new(500, 2)); // $5.00
    }

    // Test attempting a transaction on a locked account
    #[test]
    fn test_transaction_on_locked_account() {
        let mut accounts = HashMap::new();
        let mut transactions = HashMap::new();
        let mut disputes = HashSet::new();

        // Create and lock the account
        let mut account = Account::new();
        account.locked = true;
        accounts.insert(1, account);

        // Attempt to deposit into a locked account
        let deposit_record = Record {
            tx_type: "deposit".to_string(),
            client: 1,
            tx: 1,
            amount: Some(Decimal::new(500, 2)),
        };
        let result = process_transaction(
            &deposit_record,
            &mut accounts,
            &mut transactions,
            &mut disputes,
        );
        assert!(result.is_err());

        let account = accounts.get(&1).unwrap();
        // Ensure the account balance hasn't changed
        assert_eq!(account.available, Decimal::new(0, 2)); // $0.00
        assert_eq!(account.total, Decimal::new(0, 2)); // $0.00
    }
}
