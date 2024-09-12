use csv::Reader;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;

#[derive(Debug)]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl TxType {
    fn from_str(s: &str) -> Result<TxType, &'static str> {
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
    let file = File::open("transactions.csv")?;
    let mut rdr = Reader::from_reader(file);

    let mut accounts: HashMap<u16, Account> = HashMap::new();
    let mut transactions: HashMap<u32, Record> = HashMap::new();
    let mut disputes: HashSet<u32> = HashSet::new();

    // Stream the CSV file to avoid loading the entire file into memory
    // This allows us to handle large files or even infinite streams
    for result in rdr.deserialize() {
        let record: Record = result?;
        if let Err(e) = process_transaction(record, &mut accounts, &mut transactions, &mut disputes)
        {
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

    // Check if the account exists
    if !accounts.contains_key(&record.client) {
        match tx_type {
            TxType::Deposit => {
                // Create a new account only if it's a deposit
                accounts.insert(record.client, Account::new());
            }
            _ => {
                return Err(format!(
                    "Account {} does not exist for transaction type {:?}",
                    record.client, tx_type
                )
                .into())
            }
        }
    }

    // Unwrap is safe here because we just checked that the account exists
    let account = accounts.get_mut(&record.client).unwrap();

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
        return Err("Cannot process transaction on a locked account".into());
    }

    if transactions.contains_key(&record.tx) {
        return Err("Transaction number already exists".into());
    }

    if let Some(amount) = record.amount {
        if !has_valid_precision(&amount) {
            return Err("Deposit transaction amount exceeds allowed precision".into());
        }

        account.deposit(amount);
        transactions.insert(record.tx, record);
        Ok(())
    } else {
        Err("Deposit transaction missing amount".into())
    }
}

fn process_withdrawal(
    record: Record,
    account: &mut Account,
    transactions: &mut HashMap<u32, Record>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err("Cannot process transaction on a locked account".into());
    }

    if transactions.contains_key(&record.tx) {
        return Err("Transaction number already exists".into());
    }

    if let Some(amount) = record.amount {
        if !has_valid_precision(&amount) {
            return Err("Withdrawal transaction amount exceeds allowed precision".into());
        }

        account.withdraw(amount)?;
        transactions.insert(record.tx, record);
        Ok(())
    } else {
        Err("Withdrawal transaction missing amount".into())
    }
}

fn process_dispute(
    record: Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err("Cannot process transaction on a locked account".into());
    }

    let disputed_tx = transactions
        .get(&record.tx)
        .ok_or("Disputed transaction not found")?;

    if disputed_tx.tx_type.to_lowercase() != "deposit" {
        return Err("Only deposit transactions can be disputed".into());
    }

    if disputes.contains(&record.tx) {
        return Err("Transaction is already in dispute".into());
    }

    if let Some(amount) = disputed_tx.amount {
        account.apply_dispute(amount)?;
        disputes.insert(record.tx);
        Ok(())
    } else {
        Err("Disputed transaction has no amount".into())
    }
}

fn process_resolve(
    record: Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err("Cannot process transaction on a locked account".into());
    }

    let disputed_tx = transactions
        .get(&record.tx)
        .ok_or("Resolved transaction not found")?;

    if !disputes.contains(&record.tx) {
        return Err("Transaction is not currently in dispute".into());
    }

    if let Some(amount) = disputed_tx.amount {
        account.resolve_dispute(amount)?;
        disputes.remove(&record.tx);
        Ok(())
    } else {
        Err("Resolved transaction has no amount".into())
    }
}

fn process_chargeback(
    record: Record,
    account: &mut Account,
    transactions: &HashMap<u32, Record>,
    disputes: &mut HashSet<u32>,
) -> Result<(), Box<dyn Error>> {
    if account.locked {
        return Err("Cannot process transaction on a locked account".into());
    }

    let disputed_tx = transactions
        .get(&record.tx)
        .ok_or("Chargeback transaction not found")?;
    if !disputes.contains(&record.tx) {
        return Err("Transaction is not currently in dispute".into());
    }

    if let Some(amount) = disputed_tx.amount {
        account.chargeback(amount);
        disputes.remove(&record.tx);
        Ok(())
    } else {
        Err("Chargeback transaction has no amount".into())
    }
}

fn write_accounts_to_csv(accounts: &HashMap<u16, Account>) -> Result<(), Box<dyn Error>> {
    let mut wtr = csv::Writer::from_path("accounts.csv")?;
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
