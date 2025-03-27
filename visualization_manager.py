import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

class VisualizationManager:
    """Manages data visualization functionality."""
    
    def render_visualization_options(self, data=None):
        """Display visualization options for the given data."""
        st.header("Data Visualization")
        
        # Use provided data or get from session state
        if data is None:
            if 'last_query_result' in st.session_state and st.session_state.last_query_result is not None:
                data = st.session_state.last_query_result
            else:
                st.warning("No data available for visualization. Please run a query first.")
                return
        
        # Show data summary
        st.subheader("Data Summary")
        st.write(f"Shape: {data.shape[0]} rows × {data.shape[1]} columns")
        
        # Convert all columns to appropriate types
        data_types = {}
        for col in data.columns:
            # Try to convert to numeric
            try:
                if data[col].dtype == 'object':
                    # Check if it can be converted to numeric
                    pd.to_numeric(data[col], errors='raise')
                    data_types[col] = "Numeric"
                elif pd.api.types.is_numeric_dtype(data[col]):
                    data_types[col] = "Numeric"
                else:
                    data_types[col] = "Categorical"
            except:
                # If it can't be converted to numeric, it's categorical
                data_types[col] = "Categorical"
                
            # Check if column name suggests a date
            if any(date_hint in col.lower() for date_hint in ['date', 'time', 'year', 'month', 'day']):
                # Try to parse as datetime
                try:
                    pd.to_datetime(data[col], errors='raise')
                    data_types[col] = "DateTime"
                except:
                    pass
        
        # Display data types
        st.write("Detected column types:")
        st.json(data_types)
        
        # Select visualization type
        viz_type = st.selectbox(
            "Select Visualization Type:",
            ["Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot", "Histogram", "Box Plot", "Heatmap"]
        )
        
        # Render the selected visualization
        if viz_type == "Bar Chart":
            self.render_bar_chart(data, data_types)
        elif viz_type == "Line Chart":
            self.render_line_chart(data, data_types)
        elif viz_type == "Pie Chart":
            self.render_pie_chart(data, data_types)
        elif viz_type == "Scatter Plot":
            self.render_scatter_plot(data, data_types)
        elif viz_type == "Histogram":
            self.render_histogram(data, data_types)
        elif viz_type == "Box Plot":
            self.render_box_plot(data, data_types)
        elif viz_type == "Heatmap":
            self.render_heatmap(data, data_types)
    
    def render_bar_chart(self, data, data_types):
        """Render a bar chart for the data."""
        st.subheader("Bar Chart")
        
        # Get categorical columns for x-axis
        cat_cols = [col for col, type in data_types.items() if type in ["Categorical", "DateTime"]]
        num_cols = [col for col, type in data_types.items() if type == "Numeric"]
        
        if not cat_cols:
            st.warning("No categorical columns available for bar chart x-axis.")
            return
        
        if not num_cols:
            st.warning("No numeric columns available for bar chart y-axis.")
            return
        
        # Select columns for the chart
        x_col = st.selectbox("Select X-axis column (categorical):", cat_cols)
        y_col = st.selectbox("Select Y-axis column (numeric):", num_cols)
        
        # Optional grouping
        use_color = st.checkbox("Group by another column")
        if use_color:
            color_col = st.selectbox("Select grouping column:", cat_cols)
        else:
            color_col = None
        
        # Aggregation method
        agg_method = st.selectbox("Aggregation method:", ["sum", "mean", "count", "max", "min"])
        
        # Create the chart
        try:
            # Group data for the chart
            if color_col:
                chart_data = data.groupby([x_col, color_col])[y_col].agg(agg_method).reset_index()
                fig = px.bar(chart_data, x=x_col, y=y_col, color=color_col,
                            title=f"{agg_method.capitalize()} of {y_col} by {x_col}",
                            labels={x_col: x_col, y_col: f"{agg_method.capitalize()} of {y_col}"})
            else:
                chart_data = data.groupby(x_col)[y_col].agg(agg_method).reset_index()
                fig = px.bar(chart_data, x=x_col, y=y_col,
                            title=f"{agg_method.capitalize()} of {y_col} by {x_col}",
                            labels={x_col: x_col, y_col: f"{agg_method.capitalize()} of {y_col}"})
            
            # Show the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Show the chart data
            with st.expander("Show chart data"):
                st.write(chart_data)
        except Exception as e:
            st.error(f"Error creating bar chart: {e}")
    
    def render_line_chart(self, data, data_types):
        """Render a line chart for the data."""
        st.subheader("Line Chart")
        
        # Check if we have datetime columns for best results
        datetime_cols = [col for col, type in data_types.items() if type == "DateTime"]
        num_cols = [col for col, type in data_types.items() if type == "Numeric"]
        cat_cols = [col for col, type in data_types.items() if type == "Categorical"]
        
        if not datetime_cols and not cat_cols:
            st.warning("No suitable columns for line chart x-axis (date/time or categorical).")
            return
        
        if not num_cols:
            st.warning("No numeric columns available for line chart y-axis.")
            return
        
        # Select columns for the chart
        x_options = datetime_cols + cat_cols
        x_col = st.selectbox("Select X-axis column:", x_options)
        y_col = st.selectbox("Select Y-axis column (numeric):", num_cols)
        
        # Optional grouping
        use_color = st.checkbox("Group by another column")
        if use_color:
            color_col = st.selectbox("Select grouping column:", cat_cols)
        else:
            color_col = None
        
        # Aggregation method if x is categorical
        if x_col in cat_cols:
            agg_method = st.selectbox("Aggregation method:", ["sum", "mean", "count", "max", "min"])
        else:
            agg_method = "raw"  # No aggregation for datetime columns
        
        # Create the chart
        try:
            if agg_method != "raw":
                # Group data for the chart
                if color_col:
                    chart_data = data.groupby([x_col, color_col])[y_col].agg(agg_method).reset_index()
                    fig = px.line(chart_data, x=x_col, y=y_col, color=color_col,
                                title=f"{agg_method.capitalize()} of {y_col} by {x_col}",
                                labels={x_col: x_col, y_col: f"{agg_method.capitalize()} of {y_col}"})
                else:
                    chart_data = data.groupby(x_col)[y_col].agg(agg_method).reset_index()
                    fig = px.line(chart_data, x=x_col, y=y_col,
                                title=f"{agg_method.capitalize()} of {y_col} by {x_col}",
                                labels={x_col: x_col, y_col: f"{agg_method.capitalize()} of {y_col}"})
            else:
                # Use raw data
                if color_col:
                    fig = px.line(data, x=x_col, y=y_col, color=color_col,
                                title=f"{y_col} by {x_col}",
                                labels={x_col: x_col, y_col: y_col})
                else:
                    fig = px.line(data, x=x_col, y=y_col,
                                title=f"{y_col} by {x_col}",
                                labels={x_col: x_col, y_col: y_col})
                
                chart_data = data
            
            # Show the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Show the chart data
            with st.expander("Show chart data"):
                st.write(chart_data)
        except Exception as e:
            st.error(f"Error creating line chart: {e}")
    
    def render_pie_chart(self, data, data_types):
        """Render a pie chart for the data."""
        st.subheader("Pie Chart")
        
        # Get categorical columns for labels
        cat_cols = [col for col, type in data_types.items() if type in ["Categorical", "DateTime"]]
        num_cols = [col for col, type in data_types.items() if type == "Numeric"]
        
        if not cat_cols:
            st.warning("No categorical columns available for pie chart labels.")
            return
        
        if not num_cols:
            st.warning("No numeric columns available for pie chart values.")
            return
        
        # Select columns for the chart
        label_col = st.selectbox("Select label column (categorical):", cat_cols)
        value_col = st.selectbox("Select value column (numeric):", num_cols)
        
        # Aggregation method
        agg_method = st.selectbox("Aggregation method:", ["sum", "mean", "count", "max", "min"])
        
        # Limit number of slices for readability
        max_slices = st.slider("Maximum number of slices:", 3, 20, 10)
        
        # Create the chart
        try:
            # Group data for the chart
            chart_data = data.groupby(label_col)[value_col].agg(agg_method).reset_index()
            
            # Sort and limit slices
            chart_data = chart_data.sort_values(value_col, ascending=False)
            
            if len(chart_data) > max_slices:
                # Create "Other" category for remaining slices
                main_data = chart_data.iloc[:max_slices-1]
                other_value = chart_data.iloc[max_slices-1:][value_col].sum()
                other_row = pd.DataFrame({label_col: ["Other"], value_col: [other_value]})
                chart_data = pd.concat([main_data, other_row])
            
            # Create pie chart
            fig = px.pie(chart_data, names=label_col, values=value_col,
                        title=f"{agg_method.capitalize()} of {value_col} by {label_col}")
            
            # Show the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Show the chart data
            with st.expander("Show chart data"):
                st.write(chart_data)
        except Exception as e:
            st.error(f"Error creating pie chart: {e}")
    
    def render_scatter_plot(self, data, data_types):
        """Render a scatter plot for the data."""
        st.subheader("Scatter Plot")
        
        # Get numeric columns for x and y axes
        num_cols = [col for col, type in data_types.items() if type == "Numeric"]
        cat_cols = [col for col, type in data_types.items() if type == "Categorical"]
        
        if len(num_cols) < 2:
            st.warning("At least 2 numeric columns are required for a scatter plot.")
            return
        
        # Select columns for the chart
        x_col = st.selectbox("Select X-axis column (numeric):", num_cols, key="scatter_x")
        y_col = st.selectbox("Select Y-axis column (numeric):", [col for col in num_cols if col != x_col], key="scatter_y")
        
        # Optional color and size columns
        use_color = st.checkbox("Use color for a third dimension")
        if use_color and cat_cols:
            color_col = st.selectbox("Select color column:", cat_cols)
        else:
            color_col = None
        
        use_size = st.checkbox("Use point size for a fourth dimension")
        if use_size and len(num_cols) > 2:
            size_col = st.selectbox("Select size column (numeric):", [col for col in num_cols if col not in [x_col, y_col]])
        else:
            size_col = None
        
        # Create the chart
        try:
            if color_col and size_col:
                fig = px.scatter(data, x=x_col, y=y_col, color=color_col, size=size_col,
                                title=f"{y_col} vs {x_col} (colored by {color_col}, sized by {size_col})",
                                labels={x_col: x_col, y_col: y_col})
            elif color_col:
                fig = px.scatter(data, x=x_col, y=y_col, color=color_col,
                                title=f"{y_col} vs {x_col} (colored by {color_col})",
                                labels={x_col: x_col, y_col: y_col})
            elif size_col:
                fig = px.scatter(data, x=x_col, y=y_col, size=size_col,
                                title=f"{y_col} vs {x_col} (sized by {size_col})",
                                labels={x_col: x_col, y_col: y_col})
            else:
                fig = px.scatter(data, x=x_col, y=y_col,
                                title=f"{y_col} vs {x_col}",
                                labels={x_col: x_col, y_col: y_col})
            
            # Add trendline if requested
            show_trendline = st.checkbox("Show trendline")
            if show_trendline:
                fig.update_layout(showlegend=True)
                fig.add_trace(
                    go.Scatter(
                        x=data[x_col], 
                        y=data[y_col].where(~data[x_col].isna()),
                        mode='lines',
                        name='Trendline',
                        line=dict(color='rgba(255, 0, 0, 0.5)', width=2)
                    )
                )
            
            # Show the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Calculate correlation
            correlation = data[[x_col, y_col]].corr().iloc[0, 1]
            st.info(f"Correlation between {x_col} and {y_col}: {correlation:.4f}")
            
        except Exception as e:
            st.error(f"Error creating scatter plot: {e}")
    
    def render_histogram(self, data, data_types):
        """Render a histogram for the data."""
        st.subheader("Histogram")
        
        # Get numeric columns
        num_cols = [col for col, type in data_types.items() if type == "Numeric"]
        cat_cols = [col for col, type in data_types.items() if type == "Categorical"]
        
        if not num_cols:
            st.warning("No numeric columns available for histogram.")
            return
        
        # Select column for the histogram
        hist_col = st.selectbox("Select column for histogram:", num_cols)
        
        # Optional grouping
        use_color = st.checkbox("Group by categorical column")
        if use_color and cat_cols:
            color_col = st.selectbox("Select grouping column:", cat_cols)
        else:
            color_col = None
        
        # Number of bins
        num_bins = st.slider("Number of bins:", 5, 100, 20)
        
        # Create the histogram
        try:
            if color_col:
                fig = px.histogram(data, x=hist_col, color=color_col, nbins=num_bins,
                                title=f"Distribution of {hist_col} by {color_col}",
                                marginal="box")
            else:
                fig = px.histogram(data, x=hist_col, nbins=num_bins,
                                title=f"Distribution of {hist_col}",
                                marginal="box")
            
            # Customize layout
            fig.update_layout(bargap=0.1)
            
            # Show the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Show statistics
            st.subheader("Summary Statistics")
            stats_df = data[hist_col].describe().reset_index()
            stats_df.columns = ["Statistic", "Value"]
            st.table(stats_df)
            
        except Exception as e:
            st.error(f"Error creating histogram: {e}")
    
    def render_box_plot(self, data, data_types):
        """Render a box plot for the data."""
        st.subheader("Box Plot")
        
        # Get numeric and categorical columns
        num_cols = [col for col, type in data_types.items() if type == "Numeric"]
        cat_cols = [col for col, type in data_types.items() if type == "Categorical"]
        
        if not num_cols:
            st.warning("No numeric columns available for box plot.")
            return
        
        # Select columns for the box plot
        y_col = st.selectbox("Select numeric column for Y-axis:", num_cols)
        
        use_categories = st.checkbox("Group by categorical column")
        if use_categories and cat_cols:
            x_col = st.selectbox("Select categorical column for X-axis:", cat_cols)
            
            # Optional color grouping
            use_color = st.checkbox("Add another grouping dimension")
            if use_color:
                color_col = st.selectbox("Select color grouping column:", 
                                       [col for col in cat_cols if col != x_col])
            else:
                color_col = None
        else:
            x_col = None
            color_col = None
        
        # Create the box plot
        try:
            if x_col and color_col:
                fig = px.box(data, x=x_col, y=y_col, color=color_col,
                           title=f"Distribution of {y_col} by {x_col} and {color_col}")
            elif x_col:
                fig = px.box(data, x=x_col, y=y_col,
                           title=f"Distribution of {y_col} by {x_col}")
            else:
                fig = px.box(data, y=y_col,
                           title=f"Distribution of {y_col}")
            
            # Show the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Show statistics
            if x_col:
                st.subheader("Summary Statistics by Category")
                
                # Group by the categorical column and show statistics
                grouped_stats = data.groupby(x_col)[y_col].describe().reset_index()
                st.dataframe(grouped_stats)
            else:
                st.subheader("Summary Statistics")
                stats_df = data[y_col].describe().reset_index()
                stats_df.columns = ["Statistic", "Value"]
                st.table(stats_df)
                
        except Exception as e:
            st.error(f"Error creating box plot: {e}")
    
    def render_heatmap(self, data, data_types):
        """Render a heatmap for the data."""
        st.subheader("Correlation Heatmap")
        
        # Get numeric columns for correlation
        num_cols = [col for col, type in data_types.items() if type == "Numeric"]
        
        if len(num_cols) < 2:
            st.warning("At least 2 numeric columns are required for a correlation heatmap.")
            return
        
        # Select columns for the heatmap
        selected_cols = st.multiselect(
            "Select columns for correlation analysis:",
            num_cols,
            default=num_cols[:min(5, len(num_cols))]
        )
        
        if len(selected_cols) < 2:
            st.warning("Please select at least 2 columns.")
            return
        
        # Create the heatmap
        try:
            # Calculate correlation matrix
            corr_matrix = data[selected_cols].corr()
            
            # Create heatmap
            fig = px.imshow(
                corr_matrix,
                text_auto=True,
                color_continuous_scale="RdBu_r",
                title="Correlation Matrix",
                zmin=-1, zmax=1
            )
            
            # Show the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Show the correlation matrix
            st.subheader("Correlation Matrix")
            st.dataframe(corr_matrix.style.background_gradient(cmap="coolwarm", axis=None, vmin=-1, vmax=1))
            
        except Exception as e:
            st.error(f"Error creating heatmap: {e}")
