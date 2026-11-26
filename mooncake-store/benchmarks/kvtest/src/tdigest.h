#pragma once

#include <algorithm>
#include <cmath>
#include <vector>

namespace mooncake::kvtest {

// Buffered, mergeable t-digest using the arcsine scale. Tail centroids are
// deliberately smaller than central ones. Memory is O(compression), not O(N).
class TDigest {
 public:
    struct Centroid { double mean; double weight; };
    explicit TDigest(double compression = 1000) : compression_(compression) {}
    void Add(double mean, double weight = 1) {
        if (!std::isfinite(mean) || !std::isfinite(weight) || weight <= 0) return;
        pending_.push_back({mean, weight});
        if (pending_.size() >= static_cast<size_t>(8 * compression_)) Compress();
    }
    void Compress() {
        if (pending_.empty()) return;
        pending_.insert(pending_.end(), centroids_.begin(), centroids_.end());
        std::sort(pending_.begin(), pending_.end(),
                  [](const auto &a, const auto &b) { return a.mean < b.mean; });
        double total = 0;
        for (const auto &c : pending_) total += c.weight;
        centroids_.clear();
        double before = 0;
        auto current = pending_.front();
        for (size_t i = 1; i < pending_.size(); ++i) {
            const auto &next = pending_[i];
            if (Scale((before + current.weight + next.weight) / total) -
                    Scale(before / total) <= 1) {
                const double weight = current.weight + next.weight;
                current.mean += (next.mean - current.mean) * next.weight / weight;
                current.weight = weight;
            } else {
                centroids_.push_back(current);
                before += current.weight;
                current = next;
            }
        }
        centroids_.push_back(current);
        pending_.clear();
    }
    const std::vector<Centroid> &Centroids() { Compress(); return centroids_; }
    double Quantile(double q) {
        Compress();
        if (centroids_.empty()) return 0;
        double total = 0;
        for (const auto &c : centroids_) total += c.weight;
        double target = q * total;
        double previous = centroids_.front().weight / 2;
        if (target <= previous) return centroids_.front().mean;
        for (size_t i = 1; i < centroids_.size(); ++i) {
            double center = previous + (centroids_[i - 1].weight + centroids_[i].weight) / 2;
            if (target <= center) {
                double f = (target - previous) / (center - previous);
                return centroids_[i - 1].mean + f * (centroids_[i].mean - centroids_[i - 1].mean);
            }
            previous = center;
        }
        return centroids_.back().mean;
    }
 private:
    double Scale(double q) const {
        return compression_ * std::asin(2 * std::clamp(q, 0.0, 1.0) - 1) /
               (2 * 3.14159265358979323846);
    }
    double compression_;
    std::vector<Centroid> centroids_;
    std::vector<Centroid> pending_;
};
}  // namespace mooncake::kvtest
