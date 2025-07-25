import argparse
import logging
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_demo():
    """Run demo with real data files"""
    logger.info("🎬 Running Demo with Real Data...")

    try:
        from src.demo_real_data import ShopStreamRealDataProcessor

        # Initialize and run processor
        processor = ShopStreamRealDataProcessor()
        success = processor.run_complete_assessment()

        if success:
            logger.info("✅ Demo completed successfully")
        else:
            logger.error("❌ Demo failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Demo failed: {e}", exc_info=True)
        sys.exit(1)


def show_help():
    """Show detailed help information"""
    help_text = """
        🚀 ShopStream Data Pipeline - Help

        USAGE:
            python src/main.py [MODE]

        MODES:
            demo        Process real data files and show results (default)
            help        Show this help message

        EXAMPLES:
            # Run demo with real data
            python src/main.py demo

            # Show help
            python src/main.py help

        DATA FILES:
            Make sure the following files are in the 'data/' folder:
            • clickstream_events.json
            • transactions.csv
            • customer_support.json
            • product_catalog.json

        For more information, see README.md
        """
    print(help_text)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='ShopStream Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'mode',
        nargs='?',
        choices=['demo', 'help'],
        default='demo',
        help='Run mode (default: demo)'
    )

    args = parser.parse_args()

    # Display startup banner
    logger.info("🏪 ShopStream Data Pipeline v1.0")
    logger.info(f"🎯 Mode: {args.mode}")

    # Route to appropriate handler
    try:
        if args.mode == 'demo':
            run_demo()
        elif args.mode == 'help':
            show_help()

    except KeyboardInterrupt:
        logger.info("👋 Goodbye!")
    except Exception as e:
        logger.error(f"💥 Application failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
