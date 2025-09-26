

Process flow
flowchart TD
    A[Start ETL Process] --> B[Connect to On-Premise SQL Database]
    
    B --> C{Database Connection Successful?}
    C -->|No| C1[Log Error & Retry]
    C1 --> C
    C -->|Yes| D[Query SQL Tables]
    
    D --> E[Extract Table Schema & Data]
    E --> F[Initialize Excel Workbook via xlwings]
    
    F --> G[For Each SQL Table]
    G --> H[Create Corresponding Worksheet]
    H --> I[Map Table Columns to Sheet Columns]
    I --> J[Apply VBA Data Transformation Logic]
    
    J --> K[Write Data to Worksheet]
    K --> L[Apply Data Validation & Formatting]
    L --> M[Execute VBA Enrichment Macros]
    
    M --> N{More Tables to Process?}
    N -->|Yes| G
    N -->|No| O[Save Excel Workbook]
    
    O --> P[Initialize DuckDB Connection]
    P --> Q{DuckDB Connection Successful?}
    Q -->|No| Q1[Log Error & Retry]
    Q1 --> Q
    Q -->|Yes| R[Read Excel Sheets via xlwings]
    
    R --> S[For Each Worksheet]
    S --> T[Extract Sheet Data as DataFrame]
    T --> U[Apply Data Type Conversions]
    U --> V[Perform Data Quality Checks]
    
    V --> W{Data Quality Pass?}
    W -->|No| W1[Log Data Issues]
    W1 --> W2[Apply Data Cleansing Rules]
    W2 --> V
    W -->|Yes| X[Create/Update DuckDB Table]
    
    X --> Y[Insert Data into DuckDB]
    Y --> Z{More Sheets to Process?}
    Z -->|Yes| S
    Z -->|No| AA[Create Data Indexes]
    
    AA --> BB[Run Data Validation Queries]
    BB --> CC[Generate ETL Summary Report]
    CC --> DD[Close All Connections]
    DD --> EE[End ETL Process]
    
    %% Error Handling
    C1 --> FF[Send Alert Notification]
    Q1 --> FF
    W1 --> FF
    FF --> GG[Update ETL Log]
    
    %% Styling
    classDef startEnd fill:#e1f5fe
    classDef process fill:#f3e5f5
    classDef decision fill:#fff3e0
    classDef error fill:#ffebee
    classDef database fill:#e8f5e8
    
    class A,EE startEnd
    class B,D,E,F,H,I,J,K,L,M,O,R,T,U,X,Y,AA,BB,CC,DD process
    class C,N,Q,W,Z decision
    class C1,Q1,W1,W2,FF,GG error
    class P database