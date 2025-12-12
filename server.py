from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf

# Flask application init
app = Flask(__name__)
# CORS (Cross-Origin Resource Sharing) को सक्षम करता है, ताकि आपका React App 
# इस लोकल सर्वर से डेटा प्राप्त कर सके।
CORS(app) 

@app.route('/')
def home():
    # होम रूट पर जाने पर यह संदेश दिखाई देगा।
    return "Anand FinServ (YFinance) Backend Server is Running!"

@app.route('/get_ltp', methods=['GET'])
def get_ltp():
    """
    NSE स्टॉक के लिए लाइव LTP (Last Traded Price) प्राप्त करता है।
    यह आपके React App द्वारा कॉल किया जाने वाला मुख्य API एंडपॉइंट है।
    उदाहरण कॉल: http://localhost:5000/get_ltp?symbol=RELIANCE.NS
    """
    # URL से स्टॉक सिंबल लें (जैसे RELIANCE.NS)
    symbol = request.args.get('symbol')

    # सिंबल आवश्यक है
    if not symbol:
        return jsonify({"error": "Symbol required (e.g., RELIANCE.NS)"}), 400

    try:
        # yfinance Ticker ऑब्जेक्ट बनाएँ
        stock = yf.Ticker(symbol)
        
        # पिछले 1 दिन का डेटा 1 मिनट के अंतराल पर लें।
        # यह सबसे ताज़ा उपलब्ध क्लोजिंग कीमत (LTP) देता है।
        data = stock.history(period="1d", interval="1m")
        
        if not data.empty:
            # सबसे आखिरी (Latest) क्लोजिंग प्राइस निकालें
            price = data['Close'].iloc[-1]
            
            return jsonify({
                "status": "success",
                "symbol": symbol,
                "ltp": round(price, 2)
            })
        else:
            # अगर yfinance को सिंबल नहीं मिला
            return jsonify({"status": "error", "message": f"No data found for {symbol}. Check the symbol format (e.g., RELIANCE.NS)."}), 404
            
    except Exception as e:
        # सामान्य त्रुटि हैंडलिंग
        print(f"Error fetching data for {symbol}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # सर्वर को डिफ़ॉल्ट पोर्ट 5000 पर चलाएं। 
    # debug=True से कोई भी बदलाव करने पर सर्वर ऑटोमेटिक रीस्टार्ट हो जाएगा।
    print("Starting Flask server on http://127.0.0.1:5000/")
    app.run(debug=True, port=5000)