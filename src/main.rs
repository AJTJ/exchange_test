use csv::ReaderBuilder;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io;
use std::str::FromStr;

type ClientId = u16;
type TransactionId = u32;

// Represents the different types of transactions
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

// Represents a transaction record parsed from the CSV input
#[derive(Debug, Deserialize, Clone)]
struct Record {
    #[serde(rename = "type")]
    tx_type: String,
    client: ClientId,
    tx: TransactionId,
    #[serde(deserialize_with = "csv::invalid_option")]
    amount: Option<Decimal>,
}

// Represents a client's account, storing/managing balances and status
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

// Reads transactions from a CSV file provided as a command line argument
// Outputs the final state of all accounts in CSV format to stdout
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: cargo run -- <input_csv>");
        std::process::exit(1);
    }
    let input_file = &args[1];

    let file = File::open(input_file)?;
    let mut rdr = ReaderBuilder::new().comment(Some(b'#')).from_reader(file);

    // For the purpose of this project we'll use a HashMap to store accounts and transactions
    let mut accounts: HashMap<ClientId, Account> = HashMap::new();
    let mut transactions: HashMap<TransactionId, Record> = HashMap::new();
    let mut disputes: HashSet<TransactionId> = HashSet::new();

    // Stream each record one at a time to avoid loading the entire file into memory
    for result in rdr.deserialize() {
        let record: Record = result?;
        if let Err(e) =
            process_transaction(&record, &mut accounts, &mut transactions, &mut disputes)
        {
            // In the specification we are told to ignore invalid disputes, resolves, and chargebacks
            // so I've decided to print an error message and continue processing
            eprintln!("Failed to process transaction: {}", e);
        }
    }

    write_accounts_to_csv(&accounts)?;
    Ok(())
}

// Processes a transaction record by updating accounts and tracking transactions.
fn process_transaction(
    record: &Record,
    accounts: &mut HashMap<ClientId, Account>,
    transactions: &mut HashMap<TransactionId, Record>,
    disputes: &mut HashSet<TransactionId>,
) -> Result<(), Box<dyn Error>> {
    let tx_type = TxType::from_str(&record.tx_type)?;

    match tx_type {
        TxType::Deposit => match accounts.entry(record.client) {
            // If the account already exists, process the deposit
            Entry::Occupied(mut entry) => process_deposit(record, entry.get_mut(), transactions),
            // If the account does not exist, create a new account and process the deposit, inserting the account AFTER the deposit
            Entry::Vacant(entry) => {
                let mut account = Account::new();
                process_deposit(&record, &mut account, transactions)?;
                entry.insert(account);
                Ok(())
            }
        },

        // All other transaction types require an existing account
        TxType::Withdrawal | TxType::Dispute | TxType::Resolve | TxType::Chargeback => {
            if let Some(account) = accounts.get_mut(&record.client) {
                match tx_type {
                    TxType::Withdrawal => process_withdrawal(&record, account, transactions),
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
    transactions: &mut HashMap<TransactionId, Record>,
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

        // Reject deposits that exceed the precision
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
    transactions: &mut HashMap<TransactionId, Record>,
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

        // Reject withdrawals that exceed the precision
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

// Moves funds from available to held and records the dispute.
fn process_dispute(
    record: &Record,
    account: &mut Account,
    transactions: &HashMap<TransactionId, Record>,
    disputes: &mut HashSet<TransactionId>,
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

// Moves funds from held back to available and removes the dispute.
fn process_resolve(
    record: &Record,
    account: &mut Account,
    transactions: &HashMap<TransactionId, Record>,
    disputes: &mut HashSet<TransactionId>,
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
        Ok(())
    } else {
        Err(format!("Resolve error: Transaction {} has no amount", record.tx).into())
    }
}

// Removes held funds (and thus total funds), removes dispute and locks the account.
fn process_chargeback(
    record: &Record,
    account: &mut Account,
    transactions: &HashMap<TransactionId, Record>,
    disputes: &mut HashSet<TransactionId>,
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

// Outputs client ID, available funds, held funds, total funds, and locked status.
fn write_accounts_to_csv(accounts: &HashMap<ClientId, Account>) -> Result<(), Box<dyn Error>> {
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
    use std::env;
    use std::fs::File;
    use std::path::PathBuf;

    #[test]
    fn test_large_csv_with_edge_cases() {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let mut csv_path = PathBuf::from(manifest_dir);
        csv_path.push("tests/data/test_data.csv");

        let file = File::open(&csv_path).expect("Failed to open test data CSV file");
        let mut rdr = ReaderBuilder::new().comment(Some(b'#')).from_reader(file);

        let mut accounts: HashMap<ClientId, Account> = HashMap::new();
        let mut transactions: HashMap<TransactionId, Record> = HashMap::new();
        let mut disputes: HashSet<TransactionId> = HashSet::new();

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
                error_count += 1;
            }
        }
        assert_eq!(error_count, 13);

        let account1 = accounts.get(&1).unwrap();
        assert_eq!(account1.available, Decimal::new(130000, 2));
        assert_eq!(account1.held, Decimal::new(0, 2));
        assert_eq!(account1.total, Decimal::new(130000, 2));
        assert!(!account1.locked);

        let account2 = accounts.get(&2).unwrap();
        assert_eq!(account2.available, Decimal::new(0, 4));
        assert_eq!(account2.held, Decimal::new(0, 4));
        assert_eq!(account2.total, Decimal::new(0, 4));
        assert!(account2.locked);

        assert!(!accounts.contains_key(&3));
        assert!(!accounts.contains_key(&4));
    }

    #[test]
    fn test_account_deposit_and_withdrawal() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2)).unwrap();
        account.withdraw(Decimal::new(500, 2)).unwrap();

        assert_eq!(account.available, Decimal::new(500, 2));
        assert_eq!(account.total, Decimal::new(500, 2));
        assert_eq!(account.held, Decimal::new(0, 2));
    }

    #[test]
    fn test_account_dispute_and_resolve() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2)).unwrap();
        account.apply_dispute(Decimal::new(1000, 2)).unwrap();
        account.resolve_dispute(Decimal::new(1000, 2)).unwrap();

        assert_eq!(account.available, Decimal::new(1000, 2));
        assert_eq!(account.held, Decimal::new(0, 2));
        assert_eq!(account.total, Decimal::new(1000, 2));
    }

    #[test]
    fn test_account_chargeback() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2)).unwrap();
        account.apply_dispute(Decimal::new(1000, 2)).unwrap();
        account.chargeback(Decimal::new(1000, 2)).unwrap();

        assert_eq!(account.available, Decimal::new(0, 2));
        assert_eq!(account.held, Decimal::new(0, 2));
        assert_eq!(account.total, Decimal::new(0, 2));
        assert!(account.locked);
    }
}
