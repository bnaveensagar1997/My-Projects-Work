import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from sqlalchemy import create_engine

class StockPerformanceDashboard:
    def __init__(self, data_path):
        """
        Initialize the Stock Performance Dashboard
        
        :param data_path: Path to the directory containing stock data
        """
        self.data_path = data_path
        self.stocks_data = self.load_and_preprocess_data()
        self.sector_data = self.load_sector_data()
    
    def load_and_preprocess_data(self):
        """
        Load stock data from YAML files and transform into CSV
        
        :return: Dictionary of DataFrames, one for each stock symbol
        """
        # TODO: Implement YAML data extraction and transformation
        # This is a placeholder implementation
        stocks_data = {}
        
        # Example preprocessing steps:
        # 1. Extract data from YAML files
        # 2. Convert to DataFrame
        # 3. Calculate daily returns
        # 4. Add additional metrics
        
        return stocks_data
    
    def load_sector_data(self):
        """
        Load sector information for stocks
        
        :return: DataFrame with stock symbols and their corresponding sectors
        """
        # TODO: Implement sector data loading
        # This could be from a separate CSV file or manual mapping
        return pd.DataFrame()
    
    def calculate_volatility(self):
        """
        Calculate stock volatility based on daily returns
        
        :return: DataFrame with stock volatility metrics
        """
        volatility_data = {}
        
        for symbol, df in self.stocks_data.items():
            # Calculate daily returns
            df['daily_return'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1)
            
            # Calculate volatility (standard deviation of daily returns)
            volatility = df['daily_return'].std()
            volatility_data[symbol] = volatility
        
        # Convert to DataFrame for visualization
        volatility_df = pd.DataFrame.from_dict(volatility_data, orient='index', columns=['volatility'])
        volatility_df.sort_values('volatility', ascending=False, inplace=True)
        
        return volatility_df.head(10)  # Top 10 most volatile stocks
    
    def calculate_cumulative_returns(self):
        """
        Calculate cumulative returns for top 5 performing stocks
        
        :return: DataFrame with cumulative returns
        """
        cumulative_returns = {}
        
        for symbol, df in self.stocks_data.items():
            # Calculate cumulative returns
            df['cumulative_return'] = (1 + df['daily_return']).cumprod() - 1
            cumulative_returns[symbol] = df['cumulative_return']
        
        # Create DataFrame and identify top 5 stocks
        cumulative_df = pd.DataFrame(cumulative_returns)
        top_5_stocks = cumulative_df.iloc[-1].nlargest(5).index
        
        return cumulative_df[top_5_stocks]
    
    def sector_performance(self):
        """
        Analyze stock performance by sector
        
        :return: DataFrame with sector performance metrics
        """
        sector_performance = {}
        
        for symbol, df in self.stocks_data.items():
            # Get sector for the stock
            sector = self.sector_data.loc[symbol, 'sector']
            
            # Calculate yearly return
            yearly_return = (df['close'].iloc[-1] / df['close'].iloc[0]) - 1
            
            # Aggregate by sector
            if sector not in sector_performance:
                sector_performance[sector] = []
            sector_performance[sector].append(yearly_return)
        
        # Calculate average return by sector
        sector_avg_returns = {sector: np.mean(returns) 
                               for sector, returns in sector_performance.items()}
        
        return pd.DataFrame.from_dict(sector_avg_returns, orient='index', columns=['avg_yearly_return'])
    
    def stock_price_correlation(self):
        """
        Calculate and visualize stock price correlations
        
        :return: Correlation matrix
        """
        # Collect closing prices
        closing_prices = pd.DataFrame({
            symbol: df['close'] for symbol, df in self.stocks_data.items()
        })
        
        # Calculate correlation matrix
        correlation_matrix = closing_prices.corr()
        
        return correlation_matrix
    
    def monthly_gainers_and_losers(self):
        """
        Identify top 5 gainers and losers for each month
        
        :return: Dictionary of monthly performance DataFrames
        """
        monthly_performance = {}
        
        for symbol, df in self.stocks_data.items():
            # Group by month and calculate monthly returns
            monthly_returns = df.groupby(pd.Grouper(freq='M'))['close'].apply(
                lambda x: (x.iloc[-1] / x.iloc[0]) - 1
            )
            
            # Store monthly returns for each stock
            monthly_performance[symbol] = monthly_returns
        
        # Create monthly performance DataFrame
        monthly_df = pd.DataFrame(monthly_performance)
        
        # Prepare results for each month
        monthly_results = {}
        for month in monthly_df.index:
            month_data = monthly_df.loc[month]
            top_gainers = month_data.nlargest(5)
            top_losers = month_data.nsmallest(5)
            
            monthly_results[month] = {
                'gainers': top_gainers,
                'losers': top_losers
            }
        
        return monthly_results
    
    def create_visualizations(self):
        """
        Create various visualizations for the dashboard
        """
        # 1. Volatility Visualization
        plt.figure(figsize=(12, 6))
        volatility_data = self.calculate_volatility()
        volatility_data.plot(kind='bar', title='Top 10 Most Volatile Stocks')
        plt.tight_layout()
        plt.savefig('volatility_chart.png')
        
        # 2. Cumulative Returns Visualization
        plt.figure(figsize=(12, 6))
        cumulative_returns = self.calculate_cumulative_returns()
        cumulative_returns.plot(title='Cumulative Returns of Top 5 Stocks')
        plt.tight_layout()
        plt.savefig('cumulative_returns.png')
        
        # 3. Sector Performance Visualization
        plt.figure(figsize=(12, 6))
        sector_perf = self.sector_performance()
        sector_perf.plot(kind='bar', title='Sector Performance')
        plt.tight_layout()
        plt.savefig('sector_performance.png')
        
        # 4. Correlation Heatmap
        plt.figure(figsize=(12, 10))
        correlation_matrix = self.stock_price_correlation()
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
        plt.title('Stock Price Correlation Heatmap')
        plt.tight_layout()
        plt.savefig('correlation_heatmap.png')
    
    def save_to_database(self):
        """
        Save processed data to SQL database
        """
        # Create database connection
        engine = create_engine('postgresql://username:password@localhost/stockdb')
        
        # Save processed data to different tables
        for symbol, df in self.stocks_data.items():
            df.to_sql(f'stock_{symbol}', engine, if_exists='replace', index=False)
        
        # Save additional metrics
        volatility_data = self.calculate_volatility()
        volatility_data.to_sql('stock_volatility', engine, if_exists='replace')
        
        # Sector performance
        sector_perf = self.sector_performance()
        sector_perf.to_sql('sector_performance', engine, if_exists='replace')

def main():
    """
    Main function to run the Stock Performance Dashboard
    """
    # Initialize the dashboard
    dashboard = StockPerformanceDashboard('path/to/your/data')
    
    # Generate visualizations
    dashboard.create_visualizations()
    
    # Save processed data to database
    dashboard.save_to_database()

if __name__ == '__main__':
    main()