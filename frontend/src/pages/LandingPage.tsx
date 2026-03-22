import { Link } from "react-router-dom";

export default function LandingPage() {
  return (
    <div className="landing">
      {/* Nav */}
      <nav className="landing-nav">
        <div className="landing-nav-inner">
          <span className="landing-logo">ResiPrice</span>
          <Link to="/login" className="btn-primary">
            Sign In
          </Link>
        </div>
      </nav>

      {/* Hero */}
      <section className="hero">
        <h1>
          Apartment pricing data,
          <br />
          <span className="hero-accent">in seconds.</span>
        </h1>
        <p className="hero-sub">
          Stop spending hours clicking through apartment websites one by one.
          ResiPrice pulls live pricing, floor plans, and availability from
          multiple communities with a single click.
        </p>
        <Link to="/login" className="btn-primary btn-lg">
          Get Started
        </Link>
      </section>

      {/* Problem / Solution */}
      <section className="landing-section">
        <h2>The old way is broken</h2>
        <div className="compare-grid">
          <div className="compare-card compare-old">
            <h3>Manual Research</h3>
            <ul>
              <li>Visit each community website individually</li>
              <li>Navigate complex floor plan pages</li>
              <li>Manually copy pricing into a spreadsheet</li>
              <li>Repeat every time prices change</li>
              <li>Hours of tedious, error-prone work</li>
            </ul>
          </div>
          <div className="compare-card compare-new">
            <h3>With ResiPrice</h3>
            <ul>
              <li>Paste up to 4 community URLs</li>
              <li>Hit one button</li>
              <li>Get every unit, price, and floor plan instantly</li>
              <li>Sort, filter, and compare side by side</li>
              <li>Done in under 60 seconds</li>
            </ul>
          </div>
        </div>
      </section>

      {/* Features */}
      <section className="landing-section">
        <h2>Built for apartment professionals</h2>
        <div className="features-grid">
          <div className="feature-card">
            <div className="feature-icon">&#9889;</div>
            <h3>Real-Time Data</h3>
            <p>
              Pricing is scraped live from community websites. No stale data, no
              third-party aggregators. You see exactly what prospects see.
            </p>
          </div>
          <div className="feature-card">
            <div className="feature-icon">&#128202;</div>
            <h3>Side-by-Side Comparison</h3>
            <p>
              Compare units across multiple communities in a single sortable
              table. Filter by bedrooms, sort by rent, spot the best deals.
            </p>
          </div>
          <div className="feature-card">
            <div className="feature-icon">&#128640;</div>
            <h3>One-Click Scraping</h3>
            <p>
              Paste a URL, click Compare. ResiPrice handles the rest
              automatically -- floor tabs, SightMap widgets, iframes, APIs.
            </p>
          </div>
          <div className="feature-card">
            <div className="feature-icon">&#128176;</div>
            <h3>Competitive Intelligence</h3>
            <p>
              Know exactly what your competitors are charging. Make data-driven
              pricing decisions instead of guessing.
            </p>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="landing-section cta-section">
        <h2>Ready to stop the manual grind?</h2>
        <p>
          Get instant access to live apartment pricing data across any
          community.
        </p>
        <Link to="/login" className="btn-primary btn-lg">
          Sign In to ResiPrice
        </Link>
      </section>

      {/* Footer */}
      <footer className="landing-footer">
        <p>
          ResiPrice scrapes publicly available pricing data from apartment
          community websites.
        </p>
      </footer>
    </div>
  );
}
