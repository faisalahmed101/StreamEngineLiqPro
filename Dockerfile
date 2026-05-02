# ১. লিকুইডসোপ ইমেজ ব্যবহার
FROM savonet/liquidsoap:v2.2.5

USER root

# ২. Node.js এবং অন্যান্য টুলস ইন্সটল (Node 22 LTS বা 24 ব্যবহার করা ভালো)
RUN apt-get update && apt-get install -y \
    curl \
    ffmpeg \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ৩. ডিপেন্ডেন্সি ইন্সটল
COPY package*.json ./
RUN npm install

# ৪. প্রোজেক্ট ফাইল কপি করা
COPY . .

# ৫. ফোল্ডার পারমিশন ঠিক করা
RUN mkdir -p runtime cache && chmod -R 777 runtime cache

# ৬. পোর্ট এক্সপোজ
EXPOSE 8090

# ৭. এন্ট্রি পয়েন্ট রিসেট করা (এটিই সবচেয়ে গুরুত্বপূর্ণ লাইন)
ENTRYPOINT []

# ৮. অ্যাপ রান করা
CMD ["node", "stream_controller.js"]