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

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "type")]
    tx_type: String,
    client: u16,
    tx: u32,
    #[serde(deserialize_with = "csv::invalid_option")]
    amount: Option<Decimal>,
}

#[derive(Debug)]
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

    fn deposit(&mut self, amount: Decimal) {
        self.available += amount;
        self.total += amount;
    }

    fn withdraw(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.available >= amount {
            self.available -= amount;
            self.total -= amount;
            Ok(())
        } else {
            Err("Insufficient funds for withdrawal")
        }
    }

    fn apply_dispute(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.available >= amount {
            self.available -= amount;
            self.held += amount;
            Ok(())
        } else {
            Err("Insufficient available funds for dispute")
        }
    }

    fn resolve_dispute(&mut self, amount: Decimal) -> Result<(), &'static str> {
        if self.held >= amount {
            self.held -= amount;
            self.available += amount;
            Ok(())
        } else {
            Err("Insufficient held funds for resolve")
        }
    }

    fn chargeback(&mut self, amount: Decimal) {
        self.total -= amount;
        self.held -= amount;
        self.locked = true;
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
        if let Err(e) = process_transaction(record, &mut accounts, &mut transactions, &mut disputes)
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
    record: Record,
    accounts: &mut HashMap<u16, Account>,
    transactions: &mut HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    let tx_type = TxType::from_str(&record.tx_type)?;

    // Check if the account exists; create if it's a deposit
    if !accounts.contains_key(&record.client) && matches!(tx_type, TxType::Deposit) {
        accounts.insert(record.client, Account::new());
    }

    // If account doesn't exist after potential creation, return error
    let account = match accounts.get_mut(&record.client) {
        Some(acc) => acc,
        None => {
            return Err(format!(
                "Account {} does not exist for transaction type {:?}",
                record.client, tx_type
            )
            .into())
        }
    };

    if account.locked {
        return Err("Account is locked".into());
    }

    match tx_type {
        TxType::Deposit => process_deposit(record, account, transactions),
        TxType::Withdrawal => process_withdrawal(record, account, transactions),
        TxType::Dispute => process_dispute(record, account, transactions, disputes),
        TxType::Resolve => process_resolve(record, account, transactions, disputes),
        TxType::Chargeback => process_chargeback(record, account, transactions, disputes),
    }
}

fn process_deposit(
    record: Record,
    account: &mut Account,
    transactions: &mut HashMap<u32, Record>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err(format!("Account {} is locked; ignoring deposit", record.client).into());
    }

    if transactions.contains_key(&record.tx) {
        return Err(format!("Duplicate transation ID: {}; ignoring", record.tx).into());
    }

    if let Some(amount) = record.amount {
        if !has_valid_precision(&amount) {
            return Err(format!(
                "Deposit amount exceeds allowed precision; transaction {}",
                record.tx
            )
            .into());
        }

        account.deposit(amount);
        transactions.insert(record.tx, record);
        Ok(())
    } else {
        Err(format!("Deposit transaction {} missing amount", record.tx).into())
    }
}

fn process_withdrawal(
    record: Record,
    account: &mut Account,
    transactions: &mut HashMap<u32, Record>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err(format!("Account {} is locked; ignoring withdrawal", record.client).into());
    }

    if transactions.contains_key(&record.tx) {
        return Err(format!("Duplicate transation ID: {}; ignoring", record.tx).into());
    }

    if let Some(amount) = record.amount {
        if !has_valid_precision(&amount) {
            return Err(format!(
                "Withdrawal amount exceeds allowed precision; transaction {}",
                record.tx
            )
            .into());
        }

        account.withdraw(amount)?;
        transactions.insert(record.tx, record);
        Ok(())
    } else {
        Err(format!("Withdrawal transaction {} missing amount", record.tx).into())
    }
}

fn process_dispute(
    record: Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err(format!("Account {} is locked; ignoring dispute", record.client).into());
    }

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
    record: Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err(format!("Account {} is locked; ignoring resolve", record.client).into());
    }

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

fn process_chargeback(
    record: Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err(format!("Account {} is locked; ignoring chargeback", record.client).into());
    }

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
        account.chargeback(amount);
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

    #[test]
    fn test_deposit() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2)); // $10.00
        assert_eq!(account.available, Decimal::new(1000, 2));
        assert_eq!(account.total, Decimal::new(1000, 2));
    }

    #[test]
    fn test_withdrawal() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2));
        assert!(account.withdraw(Decimal::new(500, 2)).is_ok());
        assert_eq!(account.available, Decimal::new(500, 2));
        assert_eq!(account.total, Decimal::new(500, 2));
    }

    #[test]
    fn test_overdraft() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2));
        assert!(account.withdraw(Decimal::new(1500, 2)).is_err());
        assert_eq!(account.available, Decimal::new(1000, 2));
        assert_eq!(account.total, Decimal::new(1000, 2));
    }

    #[test]
    fn test_dispute() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2));
        assert!(account.apply_dispute(Decimal::new(500, 2)).is_ok());
        assert_eq!(account.available, Decimal::new(500, 2));
        assert_eq!(account.held, Decimal::new(500, 2));
        assert_eq!(account.total, Decimal::new(1000, 2));
    }

    #[test]
    fn test_resolve() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2));
        account.apply_dispute(Decimal::new(500, 2)).unwrap();
        assert!(account.resolve_dispute(Decimal::new(500, 2)).is_ok());
        assert_eq!(account.available, Decimal::new(1000, 2));
        assert_eq!(account.held, Decimal::new(0, 2));
        assert_eq!(account.total, Decimal::new(1000, 2));
    }

    #[test]
    fn test_chargeback() {
        let mut account = Account::new();
        account.deposit(Decimal::new(1000, 2));
        account.apply_dispute(Decimal::new(500, 2)).unwrap();
        account.chargeback(Decimal::new(500, 2));
        assert_eq!(account.available, Decimal::new(500, 2));
        assert_eq!(account.held, Decimal::new(0, 2));
        assert_eq!(account.total, Decimal::new(500, 2));
        assert!(account.locked);
    }
}
