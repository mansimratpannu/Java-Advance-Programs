import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.stream.*;
import java.util.function.*;
import java.io.*;
import java.time.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

// ==================== SCHEMA & TABLE DEFINITIONS ====================
enum DataType {
    INTEGER, STRING, DOUBLE, BOOLEAN, DATE, BLOB
}

class Column {
    private final String name;
    private final DataType type;
    private final boolean nullable;
    private final boolean unique;
    private final Object defaultValue;
    
    public Column(String name, DataType type, boolean nullable, boolean unique, Object defaultValue) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.unique = unique;
        this.defaultValue = defaultValue;
    }
    
    public String getName() { return name; }
    public DataType getType() { return type; }
    public boolean isNullable() { return nullable; }
    public boolean isUnique() { return unique; }
    public Object getDefaultValue() { return defaultValue; }
    
    @Override
    public String toString() {
        return String.format("%s %s%s%s", name, type, 
            !nullable ? " NOT NULL" : "", unique ? " UNIQUE" : "");
    }
}

class Schema {
    private final String tableName;
    private final List<Column> columns;
    private final List<String> primaryKeys;
    private final Map<String, List<String>> indexes;
    
    public Schema(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
        this.primaryKeys = new ArrayList<>();
        this.indexes = new ConcurrentHashMap<>();
    }
    
    public void addColumn(Column column) {
        columns.add(column);
    }
    
    public void addPrimaryKey(String columnName) {
        primaryKeys.add(columnName);
    }
    
    public void addIndex(String indexName, List<String> columnNames) {
        indexes.put(indexName, columnNames);
    }
    
    public String getTableName() { return tableName; }
    public List<Column> getColumns() { return columns; }
    public List<String> getPrimaryKeys() { return primaryKeys; }
    public Map<String, List<String>> getIndexes() { return indexes; }
    
    public Column getColumn(String name) {
        return columns.stream()
            .filter(c -> c.getName().equals(name))
            .findFirst()
            .orElse(null);
    }
}

class Row {
    private final Map<String, Object> data;
    private final long rowId;
    private final Instant createdAt;
    private Instant updatedAt;
    private int version;
    
    private static final AtomicLong rowIdGenerator = new AtomicLong(0);
    
    public Row() {
        this.data = new ConcurrentHashMap<>();
        this.rowId = rowIdGenerator.incrementAndGet();
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
        this.version = 0;
    }
    
    public void set(String column, Object value) {
        data.put(column, value);
        updatedAt = Instant.now();
        version++;
    }
    
    public Object get(String column) {
        return data.get(column);
    }
    
    public Map<String, Object> getData() { return new HashMap<>(data); }
    public long getRowId() { return rowId; }
    public int getVersion() { return version; }
    
    @Override
    public String toString() {
        return data.toString();
    }
}

// ==================== INDEX MANAGEMENT ====================
interface Index {
    void insert(Object key, Row row);
    void remove(Object key, Row row);
    List<Row> search(Object key);
    List<Row> rangeScan(Object start, Object end);
}

class BTreeIndex implements Index {
    private final TreeMap<Object, List<Row>> index;
    private final String columnName;
    
    public BTreeIndex(String columnName) {
        this.columnName = columnName;
        this.index = new TreeMap<>();
    }
    
    @Override
    public synchronized void insert(Object key, Row row) {
        index.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(row);
    }
    
    @Override
    public synchronized void remove(Object key, Row row) {
        List<Row> rows = index.get(key);
        if (rows != null) {
            rows.remove(row);
            if (rows.isEmpty()) {
                index.remove(key);
            }
        }
    }
    
    @Override
    public List<Row> search(Object key) {
        List<Row> rows = index.get(key);
        return rows != null ? new ArrayList<>(rows) : new ArrayList<>();
    }
    
    @Override
    public List<Row> rangeScan(Object start, Object end) {
        return index.subMap(start, true, end, true).values().stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }
}

class HashIndex implements Index {
    private final Map<Object, List<Row>> index;
    
    public HashIndex() {
        this.index = new ConcurrentHashMap<>();
    }
    
    @Override
    public void insert(Object key, Row row) {
        index.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(row);
    }
    
    @Override
    public void remove(Object key, Row row) {
        List<Row> rows = index.get(key);
        if (rows != null) {
            rows.remove(row);
            if (rows.isEmpty()) {
                index.remove(key);
            }
        }
    }
    
    @Override
    public List<Row> search(Object key) {
        List<Row> rows = index.get(key);
        return rows != null ? new ArrayList<>(rows) : new ArrayList<>();
    }
    
    @Override
    public List<Row> rangeScan(Object start, Object end) {
        throw new UnsupportedOperationException("Hash index doesn't support range scans");
    }
}

// ==================== TRANSACTION MANAGEMENT ====================
enum IsolationLevel {
    READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
}

enum TransactionState {
    ACTIVE, COMMITTED, ABORTED, FAILED
}

class Transaction {
    private final long transactionId;
    private final IsolationLevel isolationLevel;
    private TransactionState state;
    private final Instant startTime;
    private Instant endTime;
    private final Map<String, List<Row>> readSet;
    private final Map<String, List<Row>> writeSet;
    private final List<Runnable> undoLog;
    
    private static final AtomicLong txIdGenerator = new AtomicLong(0);
    
    public Transaction(IsolationLevel isolationLevel) {
        this.transactionId = txIdGenerator.incrementAndGet();
        this.isolationLevel = isolationLevel;
        this.state = TransactionState.ACTIVE;
        this.startTime = Instant.now();
        this.readSet = new ConcurrentHashMap<>();
        this.writeSet = new ConcurrentHashMap<>();
        this.undoLog = new ArrayList<>();
    }
    
    public void addToReadSet(String table, Row row) {
        readSet.computeIfAbsent(table, k -> new CopyOnWriteArrayList<>()).add(row);
    }
    
    public void addToWriteSet(String table, Row row) {
        writeSet.computeIfAbsent(table, k -> new CopyOnWriteArrayList<>()).add(row);
    }
    
    public void addUndoOperation(Runnable operation) {
        undoLog.add(operation);
    }
    
    public void rollback() {
        Collections.reverse(undoLog);
        undoLog.forEach(Runnable::run);
        state = TransactionState.ABORTED;
        endTime = Instant.now();
    }
    
    public void commit() {
        state = TransactionState.COMMITTED;
        endTime = Instant.now();
    }
    
    public long getTransactionId() { return transactionId; }
    public IsolationLevel getIsolationLevel() { return isolationLevel; }
    public TransactionState getState() { return state; }
    public void setState(TransactionState state) { this.state = state; }
    
    @Override
    public String toString() {
        return String.format("Transaction[id=%d, state=%s, isolation=%s]", 
            transactionId, state, isolationLevel);
    }
}

class TransactionManager {
    private final Map<Long, Transaction> activeTransactions;
    private final ReentrantReadWriteLock lock;
    
    public TransactionManager() {
        this.activeTransactions = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }
    
    public Transaction beginTransaction(IsolationLevel isolationLevel) {
        Transaction tx = new Transaction(isolationLevel);
        activeTransactions.put(tx.getTransactionId(), tx);
        System.out.println("Started: " + tx);
        return tx;
    }
    
    public void commit(Transaction tx) throws Exception {
        lock.writeLock().lock();
        try {
            if (tx.getState() != TransactionState.ACTIVE) {
                throw new IllegalStateException("Transaction is not active");
            }
            
            tx.commit();
            activeTransactions.remove(tx.getTransactionId());
            System.out.println("Committed: " + tx);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public void rollback(Transaction tx) {
        lock.writeLock().lock();
        try {
            tx.rollback();
            activeTransactions.remove(tx.getTransactionId());
            System.out.println("Rolled back: " + tx);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public boolean hasConflict(Transaction tx) {
        // Simplified conflict detection
        return false;
    }
}

// ==================== QUERY ENGINE ====================
enum Operator {
    EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_EQUAL, LESS_EQUAL, LIKE, IN
}

class Condition {
    private final String column;
    private final Operator operator;
    private final Object value;
    
    public Condition(String column, Operator operator, Object value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
    
    @SuppressWarnings("unchecked")
    public boolean evaluate(Row row) {
        Object columnValue = row.get(column);
        if (columnValue == null) return false;
        
        switch (operator) {
            case EQUALS:
                return columnValue.equals(value);
            case NOT_EQUALS:
                return !columnValue.equals(value);
            case GREATER_THAN:
                return ((Comparable) columnValue).compareTo(value) > 0;
            case LESS_THAN:
                return ((Comparable) columnValue).compareTo(value) < 0;
            case GREATER_EQUAL:
                return ((Comparable) columnValue).compareTo(value) >= 0;
            case LESS_EQUAL:
                return ((Comparable) columnValue).compareTo(value) <= 0;
            case LIKE:
                return columnValue.toString().contains(value.toString());
            default:
                return false;
        }
    }
    
    public String getColumn() { return column; }
    public Operator getOperator() { return operator; }
    public Object getValue() { return value; }
}

class Query {
    private String tableName;
    private List<String> selectColumns;
    private List<Condition> conditions;
    private String orderByColumn;
    private boolean ascending;
    private int limit;
    private int offset;
    
    public Query(String tableName) {
        this.tableName = tableName;
        this.selectColumns = new ArrayList<>();
        this.conditions = new ArrayList<>();
        this.ascending = true;
        this.limit = -1;
        this.offset = 0;
    }
    
    public Query select(String... columns) {
        selectColumns.addAll(Arrays.asList(columns));
        return this;
    }
    
    public Query where(String column, Operator operator, Object value) {
        conditions.add(new Condition(column, operator, value));
        return this;
    }
    
    public Query orderBy(String column, boolean ascending) {
        this.orderByColumn = column;
        this.ascending = ascending;
        return this;
    }
    
    public Query limit(int limit) {
        this.limit = limit;
        return this;
    }
    
    public Query offset(int offset) {
        this.offset = offset;
        return this;
    }
    
    // Getters
    public String getTableName() { return tableName; }
    public List<String> getSelectColumns() { return selectColumns; }
    public List<Condition> getConditions() { return conditions; }
    public String getOrderByColumn() { return orderByColumn; }
    public boolean isAscending() { return ascending; }
    public int getLimit() { return limit; }
    public int getOffset() { return offset; }
}

class QueryOptimizer {
    public Query optimize(Query query) {
        // Simple optimization: reorder conditions to use indexes first
        // In a real system, this would analyze costs and choose optimal execution plan
        return query;
    }
}

class QueryExecutor {
    private final Table table;
    private final QueryOptimizer optimizer;
    
    public QueryExecutor(Table table) {
        this.table = table;
        this.optimizer = new QueryOptimizer();
    }
    
    public List<Row> execute(Query query) {
        Query optimizedQuery = optimizer.optimize(query);
        
        // Get initial result set
        List<Row> results = table.scan();
        
        // Apply conditions
        for (Condition condition : optimizedQuery.getConditions()) {
            results = results.stream()
                .filter(condition::evaluate)
                .collect(Collectors.toList());
        }
        
        // Order by
        if (optimizedQuery.getOrderByColumn() != null) {
            final String orderColumn = optimizedQuery.getOrderByColumn();
            final boolean asc = optimizedQuery.isAscending();
            
            results.sort((r1, r2) -> {
                Object v1 = r1.get(orderColumn);
                Object v2 = r2.get(orderColumn);
                @SuppressWarnings("unchecked")
                int cmp = ((Comparable) v1).compareTo(v2);
                return asc ? cmp : -cmp;
            });
        }
        
        // Limit and offset
        int start = optimizedQuery.getOffset();
        int end = optimizedQuery.getLimit() > 0 ? 
            start + optimizedQuery.getLimit() : results.size();
        
        if (start < results.size()) {
            end = Math.min(end, results.size());
            results = results.subList(start, end);
        } else {
            results = new ArrayList<>();
        }
        
        return results;
    }
}

// ==================== TABLE ====================
class Table {
    private final Schema schema;
    private final List<Row> rows;
    private final Map<String, Index> indexes;
    private final ReentrantReadWriteLock lock;
    
    public Table(Schema schema) {
        this.schema = schema;
        this.rows = new CopyOnWriteArrayList<>();
        this.indexes = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        
        // Create default indexes for primary keys
        for (String pkColumn : schema.getPrimaryKeys()) {
            indexes.put("pk_" + pkColumn, new BTreeIndex(pkColumn));
        }
    }
    
    public void createIndex(String indexName, String columnName, boolean btree) {
        Index index = btree ? new BTreeIndex(columnName) : new HashIndex();
        indexes.put(indexName, index);
        
        // Build index for existing rows
        for (Row row : rows) {
            Object key = row.get(columnName);
            if (key != null) {
                index.insert(key, row);
            }
        }
        
        System.out.println("Created index: " + indexName + " on " + schema.getTableName());
    }
    
    public void insert(Row row, Transaction tx) throws Exception {
        lock.writeLock().lock();
        try {
            validateRow(row);
            
            rows.add(row);
            
            // Update indexes
            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                String indexName = entry.getKey();
                String columnName = indexName.startsWith("pk_") ? 
                    indexName.substring(3) : indexName;
                
                Object key = row.get(columnName);
                if (key != null) {
                    entry.getValue().insert(key, row);
                }
            }
            
            if (tx != null) {
                tx.addToWriteSet(schema.getTableName(), row);
                tx.addUndoOperation(() -> rows.remove(row));
            }
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public void update(Row row, Map<String, Object> updates, Transaction tx) {
        lock.writeLock().lock();
        try {
            Map<String, Object> oldData = row.getData();
            
            for (Map.Entry<String, Object> entry : updates.entrySet()) {
                row.set(entry.getKey(), entry.getValue());
            }
            
            if (tx != null) {
                tx.addToWriteSet(schema.getTableName(), row);
                tx.addUndoOperation(() -> {
                    for (Map.Entry<String, Object> entry : oldData.entrySet()) {
                        row.set(entry.getKey(), entry.getValue());
                    }
                });
            }
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public void delete(Row row, Transaction tx) {
        lock.writeLock().lock();
        try {
            rows.remove(row);
            
            // Update indexes
            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                String columnName = entry.getKey().startsWith("pk_") ? 
                    entry.getKey().substring(3) : entry.getKey();
                Object key = row.get(columnName);
                if (key != null) {
                    entry.getValue().remove(key, row);
                }
            }
            
            if (tx != null) {
                tx.addToWriteSet(schema.getTableName(), row);
                tx.addUndoOperation(() -> rows.add(row));
            }
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public List<Row> scan() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(rows);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    private void validateRow(Row row) throws Exception {
        for (Column column : schema.getColumns()) {
            Object value = row.get(column.getName());
            
            if (value == null && !column.isNullable()) {
                throw new Exception("Column " + column.getName() + " cannot be null");
            }
            
            if (column.isUnique()) {
                Index index = indexes.get("pk_" + column.getName());
                if (index != null && !index.search(value).isEmpty()) {
                    throw new Exception("Duplicate value for unique column: " + column.getName());
                }
            }
        }
    }
    
    public Schema getSchema() { return schema; }
    public int getRowCount() { return rows.size(); }
}

// ==================== DATABASE ====================
class Database {
    private final String name;
    private final Map<String, Table> tables;
    private final TransactionManager transactionManager;
    private final ReentrantReadWriteLock lock;
    
    public Database(String name) {
        this.name = name;
        this.tables = new ConcurrentHashMap<>();
        this.transactionManager = new TransactionManager();
        this.lock = new ReentrantReadWriteLock();
    }
    
    public void createTable(Schema schema) {
        lock.writeLock().lock();
        try {
            if (tables.containsKey(schema.getTableName())) {
                throw new IllegalArgumentException("Table already exists: " + schema.getTableName());
            }
            
            Table table = new Table(schema);
            tables.put(schema.getTableName(), table);
            System.out.println("Created table: " + schema.getTableName());
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }
    
    public Transaction beginTransaction(IsolationLevel level) {
        return transactionManager.beginTransaction(level);
    }
    
    public void commit(Transaction tx) throws Exception {
        transactionManager.commit(tx);
    }
    
    public void rollback(Transaction tx) {
        transactionManager.rollback(tx);
    }
    
    public QueryExecutor getQueryExecutor(String tableName) {
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return new QueryExecutor(table);
    }
    
    public void printStats() {
        System.out.println("\n========== DATABASE STATISTICS ==========");
        System.out.println("Database: " + name);
        System.out.println("Tables: " + tables.size());
        
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            System.out.println("\nTable: " + entry.getKey());
            System.out.println("  Rows: " + entry.getValue().getRowCount());
            System.out.println("  Schema: ");
            for (Column col : entry.getValue().getSchema().getColumns()) {
                System.out.println("    " + col);
            }
        }
        System.out.println("=========================================\n");
    }
}

// ==================== MAIN APPLICATION ====================
public class AdvancedDatabaseSystem {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Advanced In-Memory Database System ===\n");
        
        // Create database
        Database db = new Database("TestDB");
        
        // Create schema
        Schema employeeSchema = new Schema("employees");
        employeeSchema.addColumn(new Column("id", DataType.INTEGER, false, true, null));
        employeeSchema.addColumn(new Column("name", DataType.STRING, false, false, null));
        employeeSchema.addColumn(new Column("age", DataType.INTEGER, true, false, 0));
        employeeSchema.addColumn(new Column("salary", DataType.DOUBLE, true, false, 0.0));
        employeeSchema.addColumn(new Column("department", DataType.STRING, true, false, null));
        employeeSchema.addPrimaryKey("id");
        
        db.createTable(employeeSchema);
        
        Table empTable = db.getTable("employees");
        
        // Create indexes
        empTable.createIndex("idx_name", "name", false);
        empTable.createIndex("idx_salary", "salary", true);
        
        // Start transaction and insert data
        Transaction tx1 = db.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        System.out.println("\nInserting employee records...");
        
        String[][] employees = {
            {"1", "Alice Johnson", "30", "75000.0", "Engineering"},
            {"2", "Bob Smith", "35", "85000.0", "Engineering"},
            {"3", "Carol White", "28", "65000.0", "Sales"},
            {"4", "David Brown", "42", "95000.0", "Management"},
            {"5", "Eve Davis", "31", "70000.0", "Sales"},
            {"6", "Frank Miller", "29", "72000.0", "Engineering"},
            {"7", "Grace Lee", "38", "88000.0", "Management"},
            {"8", "Henry Wilson", "26", "60000.0", "Sales"}
        };
        
        for (String[] emp : employees) {
            Row row = new Row();
            row.set("id", Integer.parseInt(emp[0]));
            row.set("name", emp[1]);
            row.set("age", Integer.parseInt(emp[2]));
            row.set("salary", Double.parseDouble(emp[3]));
            row.set("department", emp[4]);
            empTable.insert(row, tx1);
        }
        
        db.commit(tx1);
        System.out.println("Inserted " + employees.length + " employees");
        
        // Query 1: Select all engineers
        System.out.println("\n--- Query 1: All Engineers ---");
        Query q1 = new Query("employees")
            .select("id", "name", "salary")
            .where("department", Operator.EQUALS, "Engineering");
        
        List<Row> results1 = db.getQueryExecutor("employees").execute(q1);
        results1.forEach(row -> System.out.println(row));
        
        // Query 2: High earners (salary > 80000)
        System.out.println("\n--- Query 2: High Earners (Salary > 80000) ---");
        Query q2 = new Query("employees")
            .select("name", "salary", "department")
            .where("salary", Operator.GREATER_THAN, 80000.0)
            .orderBy("salary", false);
        
        List<Row> results2 = db.getQueryExecutor("employees").execute(q2);
        results2.forEach(row -> System.out.println(row));
        
        // Query 3: Pagination example
        System.out.println("\n--- Query 3: First 3 Employees (Ordered by Age) ---");
        Query q3 = new Query("employees")
            .select("name", "age", "department")
            .orderBy("age", true)
            .limit(3);
        
        List<Row> results3 = db.getQueryExecutor("employees").execute(q3);
        results3.forEach(row -> System.out.println(row));
        
        // Transaction with update
        System.out.println("\n--- Performing Update Transaction ---");
        Transaction tx2 = db.beginTransaction(IsolationLevel.REPEATABLE_READ);
        
        List<Row> salesPeople = db.getQueryExecutor("employees")
            .execute(new Query("employees")
                .where("department", Operator.EQUALS, "Sales"));
        
        // Give 10% raise to sales team
        for (Row row : salesPeople) {
            Double currentSalary = (Double) row.get("salary");
            Map<String, Object> updates = new HashMap<>();
            updates.put("salary", currentSalary * 1.10);
            empTable.update(row, updates, tx2);
        }
        
        db.commit(tx2);
        System.out.println("Updated " + salesPeople.size() + " sales employees");
        
        // Verify update
        System.out.println("\n--- Verifying Sales Team Salaries ---");
        Query q4 = new Query("employees")
            .select("name", "salary", "department")
            .where("department", Operator.EQUALS, "Sales")
            .orderBy("salary", false);
        
        List<Row> results4 = db.getQueryExecutor("employees").execute(q4);
        results4.forEach(row -> System.out.println(row));
        
        // Transaction rollback example
        System.out.println("\n--- Testing Transaction Rollback ---");
        Transaction tx3 = db.beginTransaction(IsolationLevel.SERIALIZABLE);
        
        Row newEmployee = new Row();
        newEmployee.set("id", 9);
        newEmployee.set("name", "Test User");
        newEmployee.set("age", 25);
        newEmployee.set("salary", 50000.0);
        newEmployee.set("department", "IT");
        empTable.insert(newEmployee, tx3);
        
        System.out.println("Inserted test employee (before rollback): " + empTable.getRowCount() + " rows");
        
        db.rollback(tx3);
        System.out.println("After rollback: " + empTable.getRowCount() + " rows");
        
        // Print final statistics
        db.printStats();
        
        System.out.println("=== Database System Demo Completed ===");
    }
}