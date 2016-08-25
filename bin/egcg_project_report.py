import sys
import os.path


if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import project_report.client
    sys.exit(project_report.client.main())
